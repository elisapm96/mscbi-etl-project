import openpyxl

def excel_to_log(excel_path, log_path):
    # Load the workbook and select the first sheet
    workbook = openpyxl.load_workbook(excel_path)
    sheet = workbook.active
    
    # Open a file to write the log
    with open(log_path, 'w') as log_file:
        # Read the headers from the first row
        headers = [cell.value for cell in sheet[1]]  # Assuming the first row is headers
        header_row = ';'.join(str(header) for header in headers)
        log_file.write(header_row + '\n')  # Write the headers as the first row in the log file

        # Iterate over the remaining rows in the sheet starting from the second row
        for row in sheet.iter_rows(min_row=2):
            values = [cell.value if cell.value is not None else '' for cell in row]
            log_entry = ';'.join(str(value) for value in values)
            log_file.write(log_entry + '\n')

# Replace 'your_excel_file.xlsx' with the path to your Excel file
# and 'output.log' with the path where you want to save the log file
excel_to_log('/Users/elisaperez/Documents/visual-studio/msc-bi/etl-project/dataset-log-file.xlsm', '/Users/elisaperez/Documents/visual-studio/msc-bi/etl-project/logfile-dataset.log')
