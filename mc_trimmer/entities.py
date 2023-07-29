import zlib
from .primitives import *


class Entity(Serializable):
    def __init__(
        self,
        length: int = 0,
        compression: int = 2,
        data: bytes = b"",
        compressed_data: bytes = b"",
    ) -> None:
        self._compression: int = compression
        self._compressed_data: bytes = compressed_data

        if length > 0:
            self.decompressed_data = zlib.decompress(data)[3:]  # 3 bytes removes root tag opening
            pass

    @classmethod
    def from_bytes(cls: type[Self], data: bytes) -> Self:
        length, compression = struct.unpack(">IB", data[: Sizes.CHUNK_HEADER_SIZE])
        nbt_data = data[Sizes.CHUNK_HEADER_SIZE :]  # Sizes.CHUNK_HEADER_SIZE + length - 1]
        assert compression == 2
        post_chunk_data = data[Sizes.CHUNK_HEADER_SIZE + length :]
        if len(post_chunk_data) > 0:
            if post_chunk_data[0] != 0:
                pass
                # print(f"Warning: post-chunk data was padded with non-zero values: {bytes(post_chunk_data[:100])}")
        return cls(length=length, compression=compression, data=nbt_data, compressed_data=data)

    def conditional_reset(self, condition: Callable[[Self], bool]) -> bool:
        if self._compressed_data != b"":
            if condition(self):
                self._compressed_data = b""
                return True
        return False

    def __bytes__(self) -> bytes:
        return bytes(self._compressed_data)

    @property
    def SIZE(self) -> int:
        return len(self._compressed_data)


class EntitiesFile(RegionLike):
    def __init__(self, location_data: bytes, timestamp_data: bytes, data: bytes) -> None:
        self.entity_data: list[ChunkDataBase[Entity]] = []
        locations: ArrayOfSerializable[SerializableLocation] = LocationData().from_bytes(location_data)
        timestamps: ArrayOfSerializable[Timestamp] = TimestampData().from_bytes(timestamp_data)

        for i, (loc, ts) in enumerate(zip(locations, timestamps, strict=False)):
            chunk: Entity
            if loc.size > 0:
                assert loc.offset >= 2
                start = loc.offset * Sizes.CHUNK_SIZE_MULTIPLIER
                entity_data = data[start : start + loc.size * Sizes.CHUNK_SIZE_MULTIPLIER]

                d = ChunkDataBase[Entity](data=Entity.from_bytes(entity_data), location=loc, timestamp=ts, index=i)
                self.entity_data.append(d)

    def __bytes__(self) -> bytes:
        return RegionLike.to_bytes(self.entity_data)

    @classmethod
    def from_file(cls, file: Path) -> Self:
        with open(file, "+rb") as f:
            data = memoryview(f.read()).toreadonly()
            chunk_location_data: bytes = data[: Sizes.LOCATION_DATA_SIZE]
            timestamps_data: bytes = data[
                Sizes.LOCATION_DATA_SIZE : Sizes.LOCATION_DATA_SIZE + Sizes.TIMESTAMPS_DATA_SIZE
            ]
            return EntitiesFile(chunk_location_data, timestamps_data, data)

    def reset_chunk(self, index: int):
        self.entity_data.remove(ChunkDataBase[Entity](Entity(), SerializableLocation(), Timestamp(), index=index))
        pass
