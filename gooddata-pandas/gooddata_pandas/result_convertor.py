# (C) 2022 GoodData Corporation
from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional, Tuple, Union

import pandas

from gooddata_afm_client import models
from gooddata_sdk import ExecutionResponse, ExecutionResult

_DEFAULT_PAGE_SIZE = 100
_DataHeaders = List[List[Any]]
_DataArray = List[Union[int, None]]


@dataclass(frozen=True)
class _DataWithHeaders:
    data: Union[_DataArray, List[_DataArray]]
    """extracted data; either array of values for one-dimensional result or array of arrays of values"""

    data_headers: Tuple[_DataHeaders, Optional[_DataHeaders]]
    """per-dimension headers for the data"""

    grand_totals: Tuple[Optional[List[_DataArray]], Optional[List[_DataArray]]]
    """per-dimension grand total data"""

    grand_total_headers: Tuple[Optional[_DataHeaders], Optional[_DataHeaders]]
    """per-dimension grand total headers"""


@dataclass
class _AccumulatedData:
    """
    Utility class to offload code from the function that extracts all data and headers for a
    particular paged result. The method drives the paging and calls out to this class to accumulate
    the essential data and headers from the page.
    """

    data: Any = field(init=False)
    data_headers: List[Optional[_DataHeaders]] = field(init=False)
    grand_totals: List[Optional[List[_DataArray]]] = field(init=False)
    grand_totals_headers: List[Optional[_DataHeaders]] = field(init=False)

    def __post_init__(self):
        self.data = []
        self.data_headers = [None, None]
        self.grand_totals = [None, None]
        self.grand_totals_headers = [None, None]

    def accumulate_data(self, from_result: ExecutionResult) -> None:
        """
        if one-dimensional result, the data is single array, so this adds the elements of that array into 'data'
        if two-dimensional, the data is array of arrays, so this adds as many arrays as there are table rows
        """
        self.data.extend(from_result.data)

    def extend_existing_row_data(self, from_result: ExecutionResult) -> None:
        offset = from_result.paging_offset[0]

        for i in range(len(from_result.data)):
            self.data[offset + i].extend(from_result.data[i])

    def accumulate_headers(self, from_result: ExecutionResult, from_dim: int) -> None:
        """
        Accumulates headers for particular dimension of a result into the provided `data_headers` array at the index
        matching the dimension index. This will mutate the `data_headers`
        """
        if self.data_headers[from_dim] is None:
            self.data_headers[from_dim] = from_result.get_all_headers(dim=from_dim)
        else:
            for idx, headers in enumerate(from_result.get_all_headers(dim=from_dim)):
                self.data_headers[from_dim][idx].extend(headers)

    @staticmethod
    def _extract_dim_idx(grand_total: models.ExecutionResultGrandTotal) -> int:
        # TODO: this is one super-nasty hack; there are two things:
        #  - grand totals list contains grand totals per-dimension but in in arbitrary order & the cardinality
        #    of the list does not match the number of dimensions of the result
        #  - for grand-totals in some dimension, the totalDimensions property mentions the dimension's
        #    localIdentifier -> fine, except that localIdentifier is _nowhere_ else in the exec response or
        #    in the exec result;
        #  - there is also this thing with the totalDimensions being an array, don't know why; guessing its relevant
        #    for grand totals??
        #  so for now doing with this nasty thing of relying on convention used in both UI and python SDK where
        #  the dimension local identifier always specified index of the dimension at the end :)
        #
        # imho the proper way to deal with this is to include dimension identifier at least in the execution response,
        # in the dimension descriptor - same as labels and metrics have their local id there; that way a proper
        # lookup can be done and identify dimension index
        dims = grand_total["totalDimensions"]
        assert len(dims) == 1

        return int(dims[0][-1])

    def accumulate_grand_totals(self, from_result: ExecutionResult, paging_dim: int) -> None:
        """
        accumulates grand totals from the results; processes all grand totals on all dimensions; the method
        needs to know in which direction is the paging happening so that it can append new grand total data.
        """
        grand_totals = from_result.grand_totals
        if not len(grand_totals):
            return

        for grand_total in grand_totals:
            dim_idx = self._extract_dim_idx(grand_total)
            # the dimension id specified on the grand total says from what dimension were
            # the grand totals calculated (1 for column totals or 0 for row totals);
            #
            # the grand totals themselves should, however be placed in the opposite dimension:
            #
            # column totals are extra rows at the end of the data
            # row totals are extra columns at the right 'edge' of the data
            opposite_dim = 1 if dim_idx == 0 else 0

            if self.grand_totals[opposite_dim] is None:
                # grand totals not initialized yet; initialize both data and headers by making
                # a shallow copy from the results
                self.grand_totals[opposite_dim] = grand_total["data"][:]
                # TODO: wtf is the deal with this? why can there be multiple elements in the headerGroups list?
                self.grand_totals_headers[opposite_dim] = grand_total["dimensionHeaders"][0]["headerGroups"][0][
                    "headers"
                ][:]
            elif paging_dim != opposite_dim:
                # grand totals are already initialized and the code is paging in the direction that reveals
                # additional grand total values; append them accordingly; no need to consider total headers:
                # that is because only the grand total data is subject to paging
                if opposite_dim == 0:
                    # have column totals and paging 'to the right'; totals for the new columns are revealed so
                    # extend existing data arrays
                    for total_idx, total_data in enumerate(grand_total["data"]):
                        self.grand_totals[opposite_dim][total_idx].extend(total_data)
                else:
                    # have row totals and paging down, keep adding extra rows
                    self.grand_totals[opposite_dim].extend(grand_total["data"])

    def result(self) -> _DataWithHeaders:
        return _DataWithHeaders(
            data=self.data,
            data_headers=(self.data_headers[0], self.data_headers[1]),
            grand_totals=(self.grand_totals[0], self.grand_totals[1]),
            grand_total_headers=(self.grand_totals_headers[0], self.grand_totals_headers[1]),
        )


def _extract_all_result_data(response: ExecutionResponse, page_size: int = _DEFAULT_PAGE_SIZE) -> _DataWithHeaders:
    """
    Extracts all data and headers for an execution result. This does page around the execution result to extract
    everything from the paged API.

    :param response: execution response to work with;
    :return:
    """
    num_dims = len(response.dimensions)
    offset = [0 for _ in range(num_dims)]
    limit = [page_size for _ in range(num_dims)]
    acc = _AccumulatedData()

    while True:
        # top-level loop pages through the first dimension;
        #
        # if one-dimensional result, it pages over an array of data
        # if two-dimensional result, it pages over table rows
        result = response.read_result(offset=offset, limit=limit)

        acc.accumulate_data(from_result=result)
        acc.accumulate_headers(from_result=result, from_dim=0)
        acc.accumulate_grand_totals(from_result=result, paging_dim=0)

        if num_dims > 1:
            # when result is two-dimensional make sure to read the column headers and column totals
            # just once - when scrolling 'to the right' for the first time;
            load_headers_and_totals = False
            if acc.data_headers[1] is None:
                acc.accumulate_headers(from_result=result, from_dim=1)
                load_headers_and_totals = True

            if not result.is_complete(dim=1):
                # have two-dimensional result (typical table) and the page does not contain
                # all the columns.
                #
                # page 'to the right' to get data from all columns, extend existing rows with that data
                offset = [offset[0], result.next_page_start(dim=1)]
                while True:
                    result = response.read_result(offset=offset, limit=limit)
                    acc.extend_existing_row_data(from_result=result)

                    if load_headers_and_totals:
                        acc.accumulate_headers(from_result=result, from_dim=1)
                        acc.accumulate_grand_totals(from_result=result, paging_dim=1)

                    if result.is_complete(dim=1):
                        break

                    offset = [offset[0], result.next_page_start(dim=1)]

        if result.is_complete(dim=0):
            break

        offset = [result.next_page_start(dim=0), 0] if num_dims > 1 else [result.next_page_start(dim=0)]

    return acc.result()


def _create_header_mapper(response: ExecutionResponse, dim: int) -> Callable[[int, Any], str]:
    dim_descriptor = response.dimensions[dim]

    def _mapper(header_idx: int, header: Any) -> str:
        if header is None:
            return ""
        elif "attributeHeader" in header:
            return header["attributeHeader"]["labelValue"]
        elif "measureHeader" in header:
            measure_idx = header["measureHeader"]["measureIndex"]
            measure_descriptor = dim_descriptor["headers"][header_idx]["measureGroupHeaders"][measure_idx]

            if "name" in measure_descriptor:
                return measure_descriptor["name"]

            return measure_descriptor["localIdentifier"]
        elif "totalHeader" in header:
            return header["totalHeader"]["function"]

    return _mapper


def _row_headers_to_index(
    headers: Tuple[_DataHeaders, Optional[_DataHeaders]], response: ExecutionResponse
) -> Optional[pandas.Index]:
    if not len(response.dimensions[0]["headers"]):
        return None

    mapper = _create_header_mapper(response, dim=0)

    return pandas.MultiIndex.from_arrays(
        [
            tuple(mapper(header_idx, header) for header in header_group)
            for header_idx, header_group in enumerate(headers[0])
        ]
    )


def _col_headers_to_index(
    headers: Tuple[_DataHeaders, Optional[_DataHeaders]], response: ExecutionResponse
) -> Optional[pandas.Index]:
    if len(response.dimensions) == 1 or not len(response.dimensions[1]["headers"]):
        return None

    mapper = _create_header_mapper(response, dim=1)

    return pandas.MultiIndex.from_arrays(
        [
            tuple(mapper(header_idx, header) for header in header_group)
            for header_idx, header_group in enumerate(headers[1])
        ]
    )


def _merge_grand_totals_into_data(extract: _DataWithHeaders) -> Union[_DataArray, List[_DataArray]]:
    """
    Merges grand totals into the extracted data. this function will mutate the extracted data, extending
    the rows and columns with grand totals. Going with mutation here so as not to copy arrays around
    """
    data: Any = extract.data

    if extract.grand_totals[0] is not None:
        # column totals are computed into extra rows, one row per column total
        # add those rows at the end of the data rows
        data.extend(extract.grand_totals[0])

    if extract.grand_totals[1] is not None:
        # row totals are computed into extra columns that should be appended to
        # existing data rows
        for row_idx, cols_to_append in enumerate(extract.grand_totals[1]):
            data[row_idx].extend(cols_to_append)

    return data


def _merge_grand_total_headers_into_headers(extract: _DataWithHeaders) -> Tuple[_DataHeaders, Optional[_DataHeaders]]:
    """
    Merges grand total headers into data headers. This function will mutate the extracted data.
    """
    headers = extract.data_headers

    for dim_idx, grand_total_headers in enumerate(extract.grand_total_headers):
        if grand_total_headers is None:
            continue

        headers[dim_idx][0].extend(grand_total_headers)
        padding = [None] * len(grand_total_headers)
        for other_headers in headers[dim_idx][1:]:
            other_headers.extend(padding)

    return headers


def convert_result_to_dataframe(response: ExecutionResponse) -> pandas.DataFrame:
    """
    Converts execution result to a pandas dataframe, maintaining the dimensionality of the result.

    Because the result itself does not contain all the necessary metadata to do the full conversion, this method
    expects that the execution _response_.

    :param response: execution response through which the result can be read and converted to a dataframe
    :return: a new dataframe
    """
    extract = _extract_all_result_data(response)
    full_data = _merge_grand_totals_into_data(extract)
    full_headers = _merge_grand_total_headers_into_headers(extract)

    return pandas.DataFrame(
        data=full_data,
        index=_row_headers_to_index(full_headers, response),
        columns=_col_headers_to_index(full_headers, response),
    )
