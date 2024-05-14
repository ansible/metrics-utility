######################################
# Code for building the spreadsheet
######################################
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows

import time

class ReportCCSPv2:
    BLACK_COLOR_HEX = "00000000"
    WHITE_COLOR_HEX = "00FFFFFF"
    BLUE_COLOR_HEX = "000000FF"
    RED_COLOR_HEX = "FF0000"
    LIGHT_BLUE_COLOR_HEX = "d4eaf3"
    GREEN_COLOR_HEX = "92d050"
    FONT = "Arial"
    PRICE_FORMAT = '$#,##0.00'

    def __init__(self, dataframe, report_period, extra_params):
        # Create the workbook and worksheet
        self.wb = Workbook()
        self.wb.remove(self.wb.active)
        self.wb.create_sheet(title="Summary")

        self.ws = self.wb.active

        self.dataframe = dataframe
        self.report_period = report_period
        self.extra_params = extra_params

        self.config = {
            'sku': extra_params['report_sku'],
            'h1_heading': {
                'value': extra_params['report_h1_heading'],
            },
            'po_number': {
                'label': 'PO Number',
                'value': extra_params['report_po_number'],
            },
            'header': [{
                'label': 'CCSP Company Name',
                'value': extra_params['report_company_name'],
            },{
                'label': 'CCSP Email',
                'value': extra_params['report_email'],
            },{
                'label': 'CCSP RHN Login',
                'value': extra_params['report_rhn_login'],
            },{
                'label': 'Report Period (YYYY-MM)',
                'value': '<autogenerated>',
            }],
        }

        default_sku_description = [
            ['SKU',
            'SKU Description',
            '',
            'Term',
            'Unit of Measure',
            'Currency',
            'MSRP'],
            [f"{extra_params['report_sku']}",
            f"{extra_params['report_sku_description']}",
            '',
            'MONTH',
            'MANAGED NODE',
            'USD',
            f"{extra_params['price_per_node']}"]
        ]

        default_column_widths = {
            1: 40,
            2: 20,
            3: 15,
            4: 15,
            5: 30,
            6: 20,
            7: 20
        }

        self.config['sku_description'] = default_sku_description
        self.config['column_widths'] = default_column_widths

    def build_spreadsheet(self):
        self._init_dimensions()
        current_row = self._build_heading_h1(1)
        current_row = self._build_header(current_row)
        current_row = self._build_po_number(current_row)
        current_row = self._build_updated_timestamp(current_row)

        # current_row = self._build_sku_description(current_row)
        current_row = self._build_data_section(current_row)

        return self.wb


    def _init_dimensions(self):
        for key, value in self.config['column_widths'].items():
            self.ws.column_dimensions[get_column_letter(key)].width = value


    def _build_heading_h1(self, current_row):
        # Merge cells and insert the h1 heading
        self.ws.merge_cells(start_row=current_row, start_column=1,
                            end_row=current_row, end_column=2)
        h1_heading_cell = self.ws.cell(row=current_row, column=1)
        h1_heading_cell.value = self.config['h1_heading']['value']

        # h1_heading_cell.fill = PatternFill("solid", fgColor=self.BLACK_COLOR_HEX)
        h1_heading_cell.font = Font(name=self.FONT,
                                    size=12,
                                    bold=True,
                                    )#color=self.WHITE_COLOR_HEX)

        # h1_heading_cell.alignment = Alignment(horizontal='center')

        current_row += 1
        return current_row

    def _build_updated_timestamp(self, current_row):
        cell = self.ws.cell(row=1, column=5)
        cell.value = f"Updated: {time.strftime('%b %d, %Y')}"

        return current_row

    def _build_po_number(self, current_row):
        # Add the h2 heading payment heading
        green_background = PatternFill("solid", fgColor=self.GREEN_COLOR_HEX)

        # PO number heading and value with green background
        cell = self.ws.cell(row=5, column=4)
        cell.value = self.config['po_number']['label']
        cell.fill = green_background

        cell = self.ws.cell(row=5, column=5)
        cell.value = self.config['po_number']['value']
        cell.fill = green_background

        return current_row

    def _build_header(self, current_row):
        # Insert the header
        for header_row in self.config['header']:
            header_label_font = Font(name=self.FONT,
                                     size=12,
                                     color=self.BLACK_COLOR_HEX)
            header_value_font = Font(name=self.FONT,
                                     size=12,
                                     color=self.BLACK_COLOR_HEX)

            cell = self.ws.cell(row=current_row, column=1)
            cell.value = header_row['label']
            cell.font = header_label_font

            cell = self.ws.cell(row=current_row, column=2)
            if header_row['label'] == "Report Period (YYYY-MM)":
                # Insert dynamic report period into the specific header column
                cell.fill = PatternFill("solid", fgColor=self.GREEN_COLOR_HEX)
                cell.value = self.report_period
            else:
                cell.fill = PatternFill("solid", fgColor=self.GREEN_COLOR_HEX)
                cell.value = header_row['value']
            cell.font = header_value_font
            current_row += 1

        return current_row

    def _build_sku_description(self, current_row):
        # Insert the header
        row_counter = 0
        for header_row in self.config['sku_description']:
            header_font = Font(name=self.FONT,
                               size=11,
                               color=self.BLACK_COLOR_HEX,
                               bold=True)
            header_border = Border(left=Side(border_style='thin',
                                             color=self.BLACK_COLOR_HEX),
                                   right=Side(border_style='thin',
                                              color=self.BLACK_COLOR_HEX),
                                   top=Side(border_style='thin',
                                            color=self.BLACK_COLOR_HEX),
                                   bottom=Side(border_style='thin',
                                               color=self.BLACK_COLOR_HEX))
            value_font = Font(name=self.FONT,
                              size=11,
                              color=self.BLACK_COLOR_HEX)
            col_counter = 0
            for col_value in header_row:
                col_counter += 1

                cell = self.ws.cell(row=current_row + row_counter, column=col_counter)
                cell.value = col_value

                if row_counter == 0:
                    # header
                    cell.font = header_font
                    cell.border = header_border
                else:
                    # row
                    cell.font = value_font

            row_counter +=1
        current_row = current_row + row_counter
        # make extra 1 row space
        current_row += 1

        return current_row

    def _build_data_section(self, current_row):
        header_font = Font(name=self.FONT,
                           size=10,
                           color=self.BLACK_COLOR_HEX,
                           bold=True)
        value_font = Font(name=self.FONT,
                          size=10,
                          color=self.BLACK_COLOR_HEX)

        dotted_border = Border(left=Side(border_style='dotted',
                                         color=self.BLACK_COLOR_HEX),
                               right=Side(border_style='dotted',
                                          color=self.BLACK_COLOR_HEX),
                               top=Side(border_style='dotted',
                                        color=self.BLACK_COLOR_HEX),
                               bottom=Side(border_style='dotted',
                                           color=self.BLACK_COLOR_HEX))

        # Rename the columns based on the template
        ccsp_report_dataframe = self.dataframe.rename(
            columns={"organization_name": "End User Company Name",
                     "mark_x": "Enter 'X' to indicate\nInteral Usage",
                     "sku_number": "SKU Number",
                     "quantity_consumed": "Quantity",
                     "sku_description": "SKU Description",
                     "unit_price": "SKU Unit Price",
                     "extended_unit_price": "SKU Extended Unit\nPrice",
                     "notes": "Notes",
                    })

        row_counter = 0
        rows = dataframe_to_rows(ccsp_report_dataframe, index=False)
        for r_idx, row in enumerate(rows, current_row):
            if row_counter > 0:
                # Set bigger height of the data columns
                rd = self.ws.row_dimensions[r_idx]
                rd.height = 25

            for c_idx, value in enumerate(row, 1):
                cell = self.ws.cell(row=r_idx, column=c_idx)
                cell.value = value
                cell.border = dotted_border
                if row_counter == 0:
                    # set header style
                    cell.font = header_font
                else:
                    # set value style
                    cell.font = value_font
                    if c_idx >= 5 and c_idx <= 7:
                        cell.fill = PatternFill("solid", fgColor=self.LIGHT_BLUE_COLOR_HEX)
                    if c_idx >= 6 and c_idx <= 7:
                        # Format all price cols
                        cell.number_format = self.PRICE_FORMAT
                    if c_idx == 7:
                        # Override the value of the extended price (number of nodes X unitp rice)
                        # Multiply columns 3x4 instead of inserting the price per org
                        cell_m_1 = self.ws.cell(row=r_idx, column=4).column_letter + str(r_idx)
                        cell_m_2 = self.ws.cell(row=r_idx, column=6).column_letter + str(r_idx)
                        cell.value = '={0}*{1}'.format(cell_m_1, cell_m_2)

            row_counter += 1

        # Generate 20 more blank rows at the end
        for r_counter in range(20):
            # Set bigger height of the data columns
            r_idx = current_row + row_counter
            rd = self.ws.row_dimensions[r_idx]
            rd.height = 25

            for c_counter in range(8):
                c_idx = c_counter + 1
                cell = self.ws.cell(row=r_idx, column=c_idx)
                cell.border = dotted_border
                cell.font = value_font

                if c_idx >= 5 and c_idx <= 7:
                    cell.fill = PatternFill("solid", fgColor=self.LIGHT_BLUE_COLOR_HEX)
                if c_idx >= 6 and c_idx <= 7:
                    # Format all price cols
                    cell.number_format = self.PRICE_FORMAT
                if c_idx == 7:
                    # Override the value of the extended price (number of nodes X unitp rice)
                    # Multiply columns 3x4 instead of inserting the price per org
                    cell_m_1 = self.ws.cell(row=r_idx, column=4).column_letter + str(r_idx)
                    cell_m_2 = self.ws.cell(row=r_idx, column=6).column_letter + str(r_idx)
                    cell.value = '={0}*{1}'.format(cell_m_1, cell_m_2)

            row_counter += 1

        if row_counter >= 2:
            # If there is at least 1 data column, insert the sum:
            first_row = current_row + 1 # ignore the header
            last_row = current_row + row_counter - 1

            # Sum description
            cell = self.ws.cell(row=2, column=6)
            cell.value = "Grand total"
            cell.fill = PatternFill("solid", fgColor=self.LIGHT_BLUE_COLOR_HEX)

            # Sum value
            cell = self.ws.cell(row=2, column=7)
            cell_sum_start = cell.column_letter + str(first_row)
            cell_sum_end = cell.column_letter + str(last_row)
            cell.value = '=SUM({0}:{1})'.format(cell_sum_start, cell_sum_end)
            cell.fill = PatternFill("solid", fgColor=self.LIGHT_BLUE_COLOR_HEX)
            cell.font = Font(name=self.FONT,
                             size=10,
                             color=self.RED_COLOR_HEX)

        return current_row + row_counter