from argparse import ArgumentParser
from dataclasses import dataclass
from enum import (
    Enum,
    IntEnum,
    IntFlag,
)
from typing import (
    Iterator,
    List,
    Tuple,
)

from blessings import Terminal
from tabulate import tabulate

t = Terminal(kind='linux', force_styling=True)
PAGE_FILE_LOCATION = '/usr/local/var/postgres/base/1471196/1471676'

argument_parser = ArgumentParser()
argument_parser.add_argument('-f', '--page-file', action='store', dest='page_file_path', type=str, required=True)
argument_parser.add_argument('-m', '--show-map', action='store_true', dest='show_map', default=False, required=False)
argument_parser.add_argument('-t', '--show-tuple-data', action='store_true', dest='show_tuple_data', default=False,
                             required=False)
argument_parser.add_argument('-s', '--cell-size-kb', action='store', type=int, dest='cell_size', default=8,
                             required=False)


def read_next_bytes(iterable: Iterator[int], n: int) -> bytes:
    return bytes([next(iterable) for _ in range(n)])


class MapCellType(Enum):
    FREE = 0
    HEADER = 1
    LINE_POINTER = 2
    TUPLE = 3


@dataclass(frozen=True)
class MapCell(object):
    free_space_proportion: float
    type: MapCellType

    @classmethod
    def from_free_space_limits(cls, free_space_offsets: Tuple[int, int], cell_limits: Tuple[int, int],
                               cell_size: int):
        free_space_lower_offset, free_space_upper_offset = free_space_offsets
        cell_lower_limit, cell_upper_limit = cell_limits

        free_space_proportion = 0.0
        cell_type = MapCellType.FREE

        if free_space_offsets == (0, 0):
            # Empty page
            free_space_proportion = 0.0
            cell_type = MapCellType.FREE
        elif cell_upper_limit < free_space_lower_offset:
            # Before start of free space
            free_space_proportion = 1.0
            cell_type = MapCellType.LINE_POINTER if cell_upper_limit > 24 else MapCellType.HEADER
        elif cell_lower_limit > free_space_upper_offset:
            # After end of free space
            free_space_proportion = 1.0
            cell_type = MapCellType.TUPLE
        elif cell_lower_limit > free_space_lower_offset and cell_upper_limit < free_space_upper_offset:
            # Free space
            free_space_proportion = 0.0
            cell_type = MapCellType.FREE
        elif cell_lower_limit < free_space_lower_offset < cell_upper_limit:
            # Partially filled space between line pointers and free space
            free_space_proportion = abs(cell_lower_limit - free_space_lower_offset) / cell_size
            cell_type = MapCellType.LINE_POINTER
        elif cell_lower_limit < free_space_upper_offset < cell_upper_limit:
            # Partially filled space between free space and tuples
            free_space_proportion = abs(cell_lower_limit - free_space_upper_offset) / cell_size
            cell_type = MapCellType.TUPLE
        return cls(free_space_proportion=free_space_proportion, type=cell_type)

    def __str__(self) -> str:
        color_func = str
        if self.type == MapCellType.TUPLE:
            color_func = t.blue
        elif self.type == MapCellType.LINE_POINTER:
            color_func = t.green
        elif self.type == MapCellType.HEADER:
            color_func = t.yellow

        if self.free_space_proportion == 0.0:
            return '·'
        elif self.free_space_proportion == 1.0:
            return color_func('▉')
        else:
            return color_func('▋')


@dataclass(frozen=True)
class PageHeader(object):
    class PdFlag(IntFlag):
        HAS_FREE_LINES = 1
        PAGE_FULL = 2
        ALL_VISIBLE = 4

    lsn: Tuple[str, str]
    checksum: int
    flags: PdFlag
    free_space_lower_offset: int
    free_space_upper_offset: int
    special: int
    page_size: int
    prune_xid: int

    @classmethod
    def read_from_page_iterator(cls, page_iterator: Iterator[int]) -> 'PageHeader':
        lsn_lower_bit = hex(int.from_bytes(read_next_bytes(page_iterator, 4), 'little'))
        lsn_upper_bit = hex(int.from_bytes(read_next_bytes(page_iterator, 4), 'little'))

        checksum = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')
        flags = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')

        free_space_lower_offset = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')
        free_space_upper_offset = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')

        special = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')
        page_size = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')
        prune_xid = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')

        return cls(
            lsn=(lsn_lower_bit, lsn_upper_bit),
            checksum=checksum,
            flags=cls.PdFlag(flags),
            free_space_lower_offset=free_space_lower_offset,
            free_space_upper_offset=free_space_upper_offset,
            special=special,
            page_size=page_size,
            prune_xid=prune_xid,
        )

    def get_table_format(self) -> str:
        return tabulate((
            ('LSN', self.lsn),
            ('Checksum', self.checksum),
            ('Flags', str(self.flags)),
            ('Free space lower offset', self.free_space_lower_offset),
            ('Free space upper offset', self.free_space_upper_offset),
            ('Special', self.special),
            ('Page size', self.page_size),
            ('Prune XID', self.prune_xid),
        ))


@dataclass(frozen=True)
class LinePointer(object):
    class Flag(IntEnum):
        UNUSED = 0
        NORMAL = 1
        REDIRECT = 2
        DEAD = 3

    tuple_length: int
    tuple_offset: int
    flag: Flag

    @classmethod
    def read_from_page_iterator(cls, page_iterator: Iterator[int]) -> 'LinePointer':
        lp = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')
        tuple_length = (lp & 0xfffe0000) >> 17
        flag = (lp & 0x18000) >> 15
        tuple_offset = lp & 0x7fff

        return cls(tuple_length=tuple_length, tuple_offset=tuple_offset, flag=cls.Flag(flag))

    @classmethod
    def read_all_from_page_iterator(cls, page_iterator: Iterator[int], free_space_lower_offset: int) -> List[
        'LinePointer']:
        line_pointer_count = (free_space_lower_offset - 24) // 4
        return [cls.read_from_page_iterator(page_iterator) for _ in range(line_pointer_count)]

    @classmethod
    def get_table_format(cls, line_pointers: List['LinePointer']) -> str:
        return tabulate([
            (index, lp.tuple_offset, lp.tuple_length, str(lp.flag))
            for index, lp in enumerate(line_pointers, start=1)
        ], headers=['LP', 'Tuple offset', 'Tuple length', 'Flag'])


@dataclass(frozen=True)
class TupleData(object):
    class Infomask2Flag(IntFlag):
        KEYS_UPDATED = 0x2000
        HOT_UPDATED = 0x4000
        ONLY_TUPLE = 0x8000

    class Infomask(IntFlag):
        HASNULL = 0x0001
        HASVARWIDTH = 0x0002
        HASEXTERNAL = 0x0004
        HASOID_OLD = 0x0008
        XMAX_KEYSHR_LOCK = 0x0010
        COMBOCID = 0x0020
        XMAX_EXCL_LOCK = 0x0040
        XMAX_LOCK_ONLY = 0x0080
        XMIN_COMMITTED = 0x0100
        XMIN_INVALID = 0x0200
        XMAX_COMMITTED = 0x0400
        XMAX_INVALID = 0x0800
        XMAX_IS_MULTI = 0x1000
        UPDATED = 0x2000
        MOVED_OFF = 0x4000
        MOVED_IN = 0x8000

    offset: int
    length: int
    t_xmin: int
    t_xmax: int
    cid: int
    ctid: Tuple[int, int]
    infomask2: Infomask2Flag
    infomask: int
    header_offset: int
    data: bytes

    @classmethod
    def read_from_page_iterator(cls, page_iterator: Iterator[int], tuple_offset: int, tuple_length: int) -> 'TupleData':
        t_xmin = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')
        t_xmax = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')
        cid = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')
        ctid_lower_bit = int.from_bytes(read_next_bytes(page_iterator, 4), 'little')
        ctid_upper_bit = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')

        infomask2 = cls.Infomask2Flag(int.from_bytes(read_next_bytes(page_iterator, 2), 'little') & 0xf800)
        infomask = cls.Infomask(int.from_bytes(read_next_bytes(page_iterator, 2), 'little'))
        header_offset = int.from_bytes(read_next_bytes(page_iterator, 2), 'little')

        data = read_next_bytes(page_iterator, tuple_length - header_offset)
        return cls(
            offset=tuple_offset,
            length=tuple_length,
            t_xmin=t_xmin,
            t_xmax=t_xmax,
            cid=cid,
            ctid=(ctid_lower_bit, ctid_upper_bit),
            infomask2=infomask2,
            infomask=infomask,
            header_offset=header_offset,
            data=data,
        )

    @classmethod
    def read_all_from_page_data(cls, page_data: bytes, tuple_offsets: List[Tuple[int, int]]) -> List['TupleData']:
        tuples = list()
        for tuple_offset, tuple_length in sorted(tuple_offsets, key=lambda x: x[0]):
            tuple_data_iter = iter(page_data[tuple_offset:])
            tuples.append(TupleData.read_from_page_iterator(page_iterator=tuple_data_iter,
                                                            tuple_offset=tuple_offset,
                                                            tuple_length=tuple_length))
        return tuples

    @classmethod
    def get_table_format(cls, tuples: List['TupleData']) -> str:
        return tabulate([
            (tuple.offset, tuple.t_xmin, tuple.t_xmax, tuple.ctid, str(tuple.infomask2), str(tuple.infomask), str(tuple.data))
            for tuple in tuples
        ], headers=['Offset', 't_xmin', 't_xmax', 'ctid', 'infomask2', 'infomask', 'Data'])


class PageViewer(object):
    def __init__(self, page_file, show_map=False, show_tuple_data=False, cell_size: int = 8):
        self._page_file = page_file
        self._show_map = show_map
        self._show_tuple_data = show_tuple_data
        self._cell_size = cell_size

    def show_pages(self):
        try:
            page_index = 0
            while page_bytes := bytes(self._page_file.read(8192)):
                page_data = iter(page_bytes)
                print(t.underline_bold(f'PAGE: {page_index}'))

                page_header = PageHeader.read_from_page_iterator(page_data)
                line_pointers = LinePointer.read_all_from_page_iterator(
                    page_iterator=page_data, free_space_lower_offset=page_header.free_space_lower_offset)

                tuple_offsets = list()
                for index, lp in enumerate(line_pointers):
                    if lp.tuple_offset >= page_header.free_space_upper_offset:
                        tuple_offsets.append((lp.tuple_offset, lp.tuple_length))

                tuples = TupleData.read_all_from_page_data(page_data=page_bytes, tuple_offsets=tuple_offsets)

                if self._show_map:
                    self.show_page_map(page_header=page_header)

                if self._show_tuple_data:
                    self.show_page_header(page_header=page_header)
                    self.show_line_pointers(line_pointers=line_pointers)
                    self.show_tuples(tuples=tuples)
                page_index += 1
        except StopIteration:
            pass

    def show_page_map(self, page_header: PageHeader):
        print(t.bold(f'Map (cell size: {self._cell_size}B)'))

        freespace_bar_length = 1024 // self._cell_size
        freespace_map = [
            MapCell.from_free_space_limits(
                free_space_offsets=(page_header.free_space_lower_offset, page_header.free_space_upper_offset),
                cell_limits=(index * self._cell_size, (index + 1) * self._cell_size - 1),
                cell_size=self._cell_size,
            )
            for index in range(freespace_bar_length * 8)
        ]

        for index in range(0, len(freespace_map), freespace_bar_length):
            print(''.join(map(str, freespace_map[index:index + freespace_bar_length])))

        print()

    def show_page_header(self, page_header: PageHeader):
        print(t.bold('Page headers'))
        print(page_header.get_table_format())
        print()

    def show_line_pointers(self, line_pointers: List[LinePointer]):
        print(t.bold('Line pointers'))
        print(LinePointer.get_table_format(line_pointers))
        print()

    def show_tuples(self, tuples: List[TupleData]):
        print(t.bold('Tuples'))
        print(TupleData.get_table_format(tuples=tuples))
        print()


if __name__ == '__main__':
    args = argument_parser.parse_args()
    with open(args.page_file_path, 'rb') as page_file:
        viewer = PageViewer(page_file, show_map=args.show_map, show_tuple_data=args.show_tuple_data,
                            cell_size=args.cell_size)
        viewer.show_pages()
