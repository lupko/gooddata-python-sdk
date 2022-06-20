# (C) 2022 GoodData Corporation
from dataclasses import dataclass
from typing import Any, Callable, List, Optional, Union

import pandas

from gooddata_sdk import ExecutionResponse

_DEFAULT_PAGE_SIZE = 100


@dataclass(frozen=True)
class _DataWithHeaders:
    data: Union[List[Union[int, None]], List[List[Union[int, None]]]]
    """extracted data; either array of values for one-dimensional result or array of arrays of values"""

    row_headers: List[List[Any]]
    """headers in the first (row) dimension; array of header groups, each header group is array of the actual headers"""

    col_headers: Optional[List[List[Any]]]
    """headers in second (col) dimension - if any; array of header groups; each header group is array of the actual
    headers"""


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

    data = []
    row_headers = []
    col_headers = []
    # indicator whether col headers were fully read; on two-dim results, the column
    # headers have to be read just once (because they are same for every row in the table)
    have_col_headers = False

    while True:
        # top-level loop pages through the first dimension;
        #
        # if one-dimensional result, it pages over an array of data
        # if two-dimensional result, it pages over table rows
        result = response.read_result(offset=offset, limit=limit)

        # if one-dimensional result, the data is single array, so this adds the elements of that array into 'data'
        # if two-dimensional, the data is array of arrays, so this adds as many arrays as there are table rows
        data.extend(result.data)

        if not len(row_headers):
            # happens on the first page; initialize row headers with values from the first dim
            row_headers = result.get_all_headers(dim=0)
        else:
            for idx, headers in enumerate(result.get_all_headers(dim=0)):
                row_headers[idx].extend(headers)

        if num_dims > 1:
            if not have_col_headers:
                col_headers.extend(result.get_all_headers(dim=1))

            if not result.is_complete(dim=1):
                # have two-dimensional result (typical table) and the page does not contain
                # all the columns.
                #
                # page 'to the right' to get all the columns and keep data for rows that are
                # on the current page
                offset = [offset[0], result.next_page_start(dim=1)]
                while True:
                    result = response.read_result(offset=offset, limit=limit)

                    for i in range(len(result.data)):
                        data[offset[0] + i].extend(result.data[i])

                    if not have_col_headers:
                        for idx, headers in enumerate(result.get_all_headers(dim=1)):
                            col_headers[idx].extend(headers)

                    if result.is_complete(dim=1):
                        break

                    offset = [offset[0], result.next_page_start(dim=1)]

            have_col_headers = True

        if result.is_complete(dim=0):
            break

        offset = [result.next_page_start(dim=0), 0] if num_dims > 1 else [result.next_page_start(dim=0)]

    return _DataWithHeaders(
        data=data,
        row_headers=row_headers,
        col_headers=col_headers if have_col_headers else None,
    )


def _create_header_mapper(response: ExecutionResponse, dim: int) -> Callable[[int, Any], str]:
    dim_descriptor = response.dimensions[dim]

    def _mapper(header_idx: int, header: Any) -> str:
        if "attributeHeader" in header:
            return header["attributeHeader"]["labelValue"]
        elif "measureHeader" in header:
            measure_idx = header["measureHeader"]["measureIndex"]
            measure_descriptor = dim_descriptor["headers"][header_idx]["measureGroupHeaders"][measure_idx]

            if "name" in measure_descriptor:
                return measure_descriptor["name"]

            return measure_descriptor["localIdentifier"]

    return _mapper


def _row_headers_to_index(_extract: _DataWithHeaders, response: ExecutionResponse) -> Optional[pandas.Index]:
    if not len(response.dimensions[0]["headers"]):
        return None

    mapper = _create_header_mapper(response, dim=0)

    return pandas.MultiIndex.from_arrays(
        [
            tuple(mapper(header_idx, header) for header in header_group)
            for header_idx, header_group in enumerate(_extract.row_headers)
        ]
    )


def _col_headers_to_index(_extract: _DataWithHeaders, response: ExecutionResponse) -> Optional[pandas.Index]:
    if len(response.dimensions) == 1 or not len(response.dimensions[1]["headers"]):
        return None

    mapper = _create_header_mapper(response, dim=1)

    return pandas.MultiIndex.from_arrays(
        [
            tuple(mapper(header_idx, header) for header in header_group)
            for header_idx, header_group in enumerate(_extract.col_headers)
        ]
    )


def convert_result_to_dataframe(response: ExecutionResponse) -> pandas.DataFrame:
    """
    Converts execution result to a pandas dataframe, maintaining the dimensionality of the result.

    Because the result itself does not contain all the necessary metadata to do the full conversion, this method
    expects that the execution _response_.

    TODO: reconsider row_headers and col_headers distinction; i started with them being separate, anticipating
     different logic for their processing when building the dataframe. however, so far it turns out as not needed;
     so when finalizing this and there is no distinction between their handling, we can unify

    :param response: execution response through which the result can be read and converted to a dataframe
    :return: a new dataframe
    """
    _extract = _extract_all_result_data(response)

    return pandas.DataFrame(
        data=_extract.data,
        index=_row_headers_to_index(_extract, response),
        columns=_col_headers_to_index(_extract, response),
    )
