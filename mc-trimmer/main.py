#!/usr/bin/env python


from abc import abstractmethod
import io
from pathlib import Path
import struct
from typing import Callable, Iterable, Self, Type, TypeVar
from enum import IntEnum
import zlib
from nbt.nbt import TAG_Compound


PATH = "./data"
T = TypeVar('T', bound='Serializable')

class Sizes(IntEnum):
    CHUNK_LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024
    CHUNK_SIZE_MULTIPLIER = 4 * 1024
    CHUNK_HEADER_SIZE = 4 + 1


class Meta(type):
    def __mul__(mcs: Type[T], i: int) -> Callable[[], 'ArrayOfSerializable[T]']:
        '''With T = Type[Serializable], T * int = ArrayofSerializable[T] of size int'''
        assert(Serializable in mcs.__mro__)
        def curry():
            return ArrayOfSerializable[mcs](mcs, i)
        return curry


class Serializable(metaclass=Meta):
    @abstractmethod
    def __bytes__(self) -> bytes:
        ...

    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Self:
        ...

    @classmethod
    @property
    @abstractmethod
    def SIZE(cls) -> int:
        ...



class ArrayOfSerializable(list[T]):
    def __init__(self, cls: type[T], len: int) -> None:
        self._len: int = len
        self._cls: Type[Serializable] = cls

    def from_bytes(self, data: bytes) -> Self:
        for i in range(self._len):
            obj: T = self._cls.from_bytes(data[i * self._cls.SIZE : (i + 1) * self._cls.SIZE]) # type: ignore
            self.append(obj)
        return self

    def __bytes__(self) -> bytes:
        ret = b""
        for data in self:
            ret += bytes(data)
        return ret


class ChunkLocation(Serializable):
    def __init__(self, offset: int, size: int) -> None:
        self.offset = offset
        self.size = size

    def __repr__(self) -> str:
        return str(f'{self.__class__}: {self.offset}+{self.size}')

    def __lt__(self, other: Self):
        return self.offset < other.offset

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        offset, size = struct.unpack(">IB", b'\x00' + data)  # 3 byte offset, 1 byte size
        return cls(offset=offset, size=size)

    def __bytes__(self) -> bytes:
        return struct.pack(">IB", self.offset, self.size)[1:]

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4

class Timestamp(Serializable):
    def __init__(self, timestamp: int) -> None:
        self.timestamp = timestamp

    def __repr__(self) -> str:
        return str(f'{self.__class__}: {self.timestamp}')

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        timestamp, = struct.unpack(">I", data)
        return cls(timestamp=timestamp)

    def __bytes__(self) -> bytes:
        return struct.pack(">I", self.timestamp)

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4

class Chunk(Serializable):
    def __init__(self, length: int = 0, compression: int = 2, data: bytes = b'') -> None:
        self._compression: int = compression
        self._compressed_data: bytes = data
        self.nbt: TAG_Compound

        if length > 0:
            decompressed_data = zlib.decompress(self._compressed_data)
            member_data = decompressed_data[3 : ]  # 3 bytes removes root tag opening
            self.nbt = TAG_Compound(buffer=io.BytesIO(member_data))
        else:
            self.nbt = TAG_Compound()

    @classmethod
    def from_bytes(cls: type[Self], data: bytes) -> Self:
        length, compression = struct.unpack(">IB", data[:Sizes.CHUNK_HEADER_SIZE])
        assert compression == 2
        return cls(length=length, compression=compression, data=data[Sizes.CHUNK_HEADER_SIZE: Sizes.CHUNK_HEADER_SIZE + length - 1])

    def conditional_reset(self, condition: Callable[[TAG_Compound], bool]) -> bool:
        if self._compressed_data != b'':
            if condition(self.nbt):
                self._compressed_data = b''
                self.nbt = TAG_Compound()
                return True
        return False

    def __bytes__(self) -> bytes:
        if self._compressed_data != b'':
            l1 = len(self._compressed_data)
            data = struct.pack(">IB", l1 + 1, self._compression) + self._compressed_data # +1 for compression scheme
            l = len(data)
            assert l1 + Sizes.CHUNK_HEADER_SIZE == l
            padding = ((4096 - (l % 4096)) % 4096)
            data = data + b'\x00' * padding
            return data
        return b''

    @property
    def SIZE(self) -> int:
        return Sizes.CHUNK_HEADER_SIZE + len(self._compressed_data)

TimestampData = Timestamp * 1024
ChunkLocationData = ChunkLocation * 1024


def get_regions(path: str | Path) -> Iterable[Path]:
    p: Path = Path(path)
    if p.exists() and p.is_dir():
        return (f for f in p.glob("*8.mca") if f.is_file())
    raise Exception(f"Invalid input <{p}>")


class Region:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.locations = ChunkLocationData().from_bytes(chunk_location_data)

        self.location_order: list[ChunkLocation] = self.locations
        self.__timestamps = TimestampData().from_bytes(timestamps_data)
        self.__data = data
        self.dirty: bool = False

        # Test:

        a = bytes(self.__timestamps)
        b = bytes(timestamps_data)
        assert(b == a)

        self.__chunks: list[Chunk] = []
        for loc in self.location_order:
            if loc.size > 0:
                # location is relative to beginning of file, so timestamp and location table have to be subtracted.
                s = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER - Sizes.CHUNK_LOCATION_DATA_SIZE - Sizes.TIMESTAMPS_DATA_SIZE
                data_slice = self.__data[s: s + loc.size * 4096]
                chunk = Chunk.from_bytes(data_slice)
                self.__chunks.append(chunk)

                b = bytes(chunk)
                a = bytes(data_slice)
            else:
                self.__chunks.append(Chunk())
        pass

    def trim(self, condition: Callable[[TAG_Compound], bool]):
        for c in self.__chunks:
            self.dirty |= c.conditional_reset(condition)


    @classmethod
    def from_file(cls, region: Path) -> Self:
        with open(region, "+rb") as f:
            chunk_location_data: bytes = memoryview(f.read(Sizes.CHUNK_LOCATION_DATA_SIZE))
            timestamps_data: bytes = memoryview(f.read(Sizes.TIMESTAMPS_DATA_SIZE))

            return Region(chunk_location_data, timestamps_data, memoryview(f.read()))

    def __bytes__(self) -> bytes:
        chunks: list[bytes] = []
        deltas: list[int] = []

        for loc, chunk, ts in zip(self.location_order, self.__chunks, self.__timestamps):
            c_data = bytes(chunk)
            size_delta: int = 0
            if loc.size > 0:
                l = len(c_data)
                size_delta = loc.size - l // 4096
                if size_delta != 0:
                    pass
                loc.size = l // 4096

                if loc.size == 0:
                    loc.offset = 0
                    ts.timestamp = 0
            deltas.append(size_delta)
            chunks.append(c_data)

        cumulative_delta = 0
        chunk_data: bytes = b''
        for loc, chunk, ts, delta in sorted(zip(self.location_order, chunks, self.__timestamps, deltas), key=lambda pair: pair[0]):
            if loc.size > 0 and cumulative_delta != 0:
                cumulative_delta = min(loc.offset, cumulative_delta)
                loc.offset -= cumulative_delta
            if delta != 0:
                cumulative_delta += delta
            chunk_data += chunk

        location_data: bytes = b''.join((bytes(x) for x in self.location_order))
        timestamp_data: bytes = b''.join((bytes(x) for x in self.__timestamps))

        return location_data + timestamp_data + chunk_data

    def save_to_file(self, region: Path) -> None:
        data = bytes(self)
        if len(data) > Sizes.CHUNK_LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE:
            with open(region, "wb") as f:
                f.write(data)
        else:
            print(f"Deleting {region}")



def start():
    for r in get_regions(PATH):
        print(r)
        region = Region.from_file(r)

        with open(r, "+rb") as f:
            a = f.read()
        b = bytes(region)

        p = r.with_name("r.-13.9.mca")
        region.save_to_file(p)
        i = 0
        y = len(a)
        z = len(b)
        a1, b1 = a.hex(), b.hex()
        assert a1 == b1

        region.trim(lambda nbt: nbt["InhabitedTime"].value < 4)# 20 * 3600 * 0.25)
        if region.dirty:
            region.save_to_file(p)
            r2 = Region.from_file(p)
            pass


    exit()


if __name__ == "__main__":
    start()
