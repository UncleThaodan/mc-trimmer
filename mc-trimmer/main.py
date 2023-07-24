#!/usr/bin/env python


from abc import abstractmethod
import io
from pathlib import Path
import struct
from typing import Any, Callable, Iterable, Self, Type, TypeVar
from enum import IntEnum
import zlib
from nbt.nbt import NBTFile, TAG_Compound
import copy


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
        if (size != 0):
            pass
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

        a = bytes(self)[Sizes.CHUNK_HEADER_SIZE:]
        b = bytes(self._compressed_data)
        y = len(a)
        z = len(b)
        assert a[:z] == b

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
            padding = 4096 - (l % 4096)
            data = data + b'\x00' * padding
            return data
        return b''

    @property
    def SIZE(self) -> int:
        return Sizes.CHUNK_HEADER_SIZE + len(self._compressed_data)


print(Timestamp.__dict__)

TimestampData = Timestamp * 1024
ChunkLocationData = ChunkLocation * 1024


def get_regions(path: str | Path) -> Iterable[Path]:
    p: Path = Path(path)
    if p.exists() and p.is_dir():
        return (f for f in p.glob("*.mca") if f.is_file())
    raise Exception(f"Invalid input <{p}>")


class Region:
    def __init__(self, chunk_location_data: bytes, timestamps_data: bytes, data: bytes) -> None:
        self.locations = ChunkLocationData().from_bytes(chunk_location_data)

        self.locaiton_order: list[ChunkLocation] = self.locations
        self.__timestamps = TimestampData().from_bytes(timestamps_data)
        self.__data = data
        self.dirty: bool = False

        # Test:

        a = bytes(self.__timestamps)
        b = bytes(timestamps_data)
        assert(b == a)

        self.__chunks: list[Chunk] = []
        for loc in self.locaiton_order:
            if loc.size > 0:
                # location is relative to beginning of file, so timestamp and location table have to be subtracted.
                data_slice = self.__data[loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER - Sizes.CHUNK_LOCATION_DATA_SIZE - Sizes.TIMESTAMPS_DATA_SIZE: ]
                chunk = Chunk.from_bytes(data_slice)
                self.__chunks.append(chunk)
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
        locations: bytes = b''
        timestamps: bytes = b''

        chunks: list[bytes] = []

        size_delta: int = 0
        i = 0

        locs = copy.copy(self.locaiton_order)
        mapping = {l : c for c, l in zip(self.__chunks, self.locaiton_order)}
        for c, loc in zip(mapping.values(), self.locaiton_order):
            c_data = bytes(c)
            l = len(c_data)
            size_delta += l // 4096 - loc.size
            loc.offset += size_delta
            assert size_delta == 0
            assert loc.size == l // 4096
            loc.size = l // 4096

            locations += bytes(loc)
            chunks.append(c_data)

            i += 1

        chunk_data = b''.join([x for _, x in sorted(zip(self.locaiton_order, chunks))])

        for t in self.__timestamps:
            timestamps += bytes(t)

        return locations + timestamps + chunk_data

    def save_to_file(self, region: Path) -> None:
        with open(region, "wb") as f:
            f.write(bytes(self))



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

        region.trim(lambda nbt: nbt["InhabitedTime"].value > 4)# 20 * 3600 * 0.25)
        if region.dirty:
            region.save_to_file(p)
            r2 = Region.from_file(p)
            pass


    exit()


if __name__ == "__main__":
    start()
