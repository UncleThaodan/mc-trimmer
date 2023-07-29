from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
import struct
from typing import Callable, Generic, Self, Type, TypeVar


class Paths:
    def __init__(self, inp: Path, outp: Path, backup: Path | None = None) -> None:
        if backup == inp:
            raise Exception("Input and backup directories cannot be the same.")
        if backup == outp:
            raise Exception("Output and backup directories cannot be the same.")
        if not inp.exists():
            raise Exception("Input directory must exist.")

        self.inp_region: Path = inp / "region"
        self.inp_poi: Path = inp / "poi"
        self.inp_entities: Path = inp / "entities"

        self.outp_region: Path = outp / "region"
        self.outp_poi: Path = outp / "poi"
        self.outp_entities: Path = outp / "entities"

        self.backup_region: Path | None = None
        self.backup_poi: Path | None = None
        self.backup_entities: Path | None = None

        if backup is not None:
            self.backup_region = backup / "region"
            self.backup_poi = backup / "poi"
            self.backup_entities = backup / "entities"

            self.backup_region.mkdir(exist_ok=True)
            self.backup_poi.mkdir(exist_ok=True)
            self.backup_entities.mkdir(exist_ok=True)

        self.inp_region.mkdir(exist_ok=True)
        self.inp_poi.mkdir(exist_ok=True)
        self.inp_entities.mkdir(exist_ok=True)

        self.outp_region.mkdir(exist_ok=True)
        self.outp_poi.mkdir(exist_ok=True)
        self.outp_entities.mkdir(exist_ok=True)


T = TypeVar("T")
S = TypeVar("S", bound="Serializable")


class Strategy(Generic[T]):
    def __init__(self, typ: bytes, payload_size: int, unpack: bytes, resulting_type: Type[T]) -> None:
        self.typ: bytes = typ
        self.payload_size: int = payload_size
        self.unpack: bytes = unpack
        self.resulting_type: Type[T] = resulting_type


BYTE_STRATEGY = Strategy(typ=b"\x01", payload_size=1, unpack=b">c", resulting_type=int)
INT_STRATEGY = Strategy(typ=b"\x03", payload_size=4, unpack=b">i", resulting_type=int)
LONG_STRATEGY = Strategy(typ=b"\x04", payload_size=8, unpack=b">Q", resulting_type=int)
FLOAT_STRATEGY = Strategy(typ=b"\x05", payload_size=4, unpack=b">f", resulting_type=float)
DOUBLE_STRATEGY = Strategy(typ=b"\x06", payload_size=8, unpack=b">d", resulting_type=float)


class Sizes(IntEnum):
    LOCATION_DATA_SIZE = 4 * 1024
    TIMESTAMPS_DATA_SIZE = 4 * 1024
    CHUNK_SIZE_MULTIPLIER = 4 * 1024
    CHUNK_HEADER_SIZE = 4 + 1


class Meta(type):
    def __mul__(mcs: Type[S], i: int) -> Callable[[], "ArrayOfSerializable[S]"]:
        """With T = Type[Serializable], T * int = ArrayofSerializable[T] of size int"""
        assert Serializable in mcs.__mro__

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


class ArrayOfSerializable(list[S]):
    def __init__(self, cls: type[S], len: int) -> None:
        self._len: int = len
        self._cls: Type[Serializable] = cls

    def from_bytes(self, data: bytes) -> Self:
        for i in range(self._len):
            obj: S = self._cls.from_bytes(data[i * self._cls.SIZE : (i + 1) * self._cls.SIZE])  # type: ignore
            self.append(obj)
        return self

    def __bytes__(self) -> bytes:
        ret = b""
        for data in self:
            ret += bytes(data)
        return ret


class SerializableLocation(Serializable):
    def __init__(self, offset: int, size: int) -> None:
        self.offset = offset
        self.size = size

    def __repr__(self) -> str:
        return str(f"{self.__class__}: {self.offset}+{self.size}")

    def __lt__(self, other: Self):
        return self.offset < other.offset

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        offset, size = struct.unpack(">IB", b"\x00" + data)  # 3 byte offset, 1 byte size
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
        return str(f"{self.__class__}: {self.timestamp}")

    @classmethod
    def from_bytes(cls, data: bytes) -> Self:
        (timestamp,) = struct.unpack(">I", data)
        return cls(timestamp=timestamp)

    def __bytes__(self) -> bytes:
        return struct.pack(">I", self.timestamp)

    @classmethod
    @property
    def SIZE(cls) -> int:
        return 4


LocationData = SerializableLocation * 1024
TimestampData = Timestamp * 1024


@dataclass
class ChunkDataBase(Generic[S]):
    data: S  # Data must be Serializable.
    location: SerializableLocation
    timestamp: Timestamp
    index: int

    def __lt__(self, other: Self) -> bool:
        return self.index < other.index


class RegionLike(ABC):
    @staticmethod
    def to_bytes(data: list[ChunkDataBase[S]]) -> bytes:
        offset: int = 2
        locations: bytes = b""
        timestamps: bytes = b""
        chunks: bytes = b""

        # Adjust offsets and sizes, store chunk data
        for cd in sorted(data, key=lambda combo: combo.location.offset):
            data_bytes: bytes = bytes(cd.data)
            length = len(data_bytes)

            assert length % 4096 == 0
            if length == 0 and cd.location.size != 0:
                pass
            cd.location.size = length // 4096
            if cd.location.size == 0:
                cd.location.offset = 0
                cd.timestamp.timestamp = 0
            else:
                chunks += data_bytes
                cd.location.offset = offset
                offset += cd.location.size

        # Convert tables to binary
        previous = 2
        for cd in sorted(data, key=lambda combo: combo.index):
            if cd.index - previous > 1:
                bytes_to_add = b"\x00\x00\x00\x00" * (cd.index - previous - 1)
                locations += bytes_to_add
                timestamps += bytes_to_add
            locations += bytes(cd.location)
            timestamps += bytes(cd.timestamp)
            previous = cd.index
        bytes_to_add = b"\x00\x00\x00\x00" * (1024 - previous - 1)
        locations += bytes_to_add
        timestamps += bytes_to_add

        return locations + timestamps + chunks


def fast_get_property(decompressed_data: bytes, name: bytes, strategy: Strategy[T]) -> T:
    """Quick-fetch property by seeking through the byte-stream.

    If a property can appear more than once, this will break!"""
    prop_sequence = strategy.typ + struct.pack(">H", len(name)) + name
    start = decompressed_data.find(prop_sequence)
    if start < 0:
        raise Exception(f"Prop '{name.decode()}' not found!")
    (value,) = struct.unpack(
        strategy.unpack,
        decompressed_data[start + len(prop_sequence) : start + len(prop_sequence) + strategy.payload_size],
    )
    return value
