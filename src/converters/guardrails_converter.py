"""
Guardrails (DGBee Excel) converter.
Converts DGBee format Excel files to JSON.
"""
import json
from pathlib import Path
from openpyxl import load_workbook
from typing import List, Dict, Any
import pandas as pd
from pathlib import Path


def convert_guardrails_to_json(file_path: str) -> dict:
    # Skip these tabs - no entity/attribute data
    SKIP_TABS = ['Glossary', 'DGBee Summary', 'API Summary', 'Data Dictionary']
    
    xl = pd.ExcelFile(file_path)
    sheets = {}
    
    for sheet_name in xl.sheet_names:
        if sheet_name in SKIP_TABS:
            continue  # Skip metadata tabs
        
        # Process data tabs and include tab name
        df = pd.read_excel(file_path, sheet_name=sheet_name)
        sheets[sheet_name] = df.to_dict('records')
    
    return {
        'source_file': Path(file_path).name,
        'sheets': sheets  # Only contains data tabs
    }
    
    # Process each "Data Elements" sheet
    for sheet_name in wb.sheetnames:
        if sheet_name.startswith('Data Elements'):
            entity_name = sheet_name.replace('Data Elements ', '').strip()
            output["sheets"][entity_name] = _convert_sheet_to_dict(wb[sheet_name])
        elif sheet_name == 'DGBee Summary':
            output["summary"] = _extract_summary(wb[sheet_name])
        elif sheet_name == 'Glossary':
            output["glossary"] = _convert_sheet_to_dict(wb[sheet_name])
    
    return json.dumps(output, indent=2)


def _convert_sheet_to_dict(sheet) -> List[Dict[str, Any]]:
    """
    Convert an Excel sheet to list of dictionaries.
    First row is headers, subsequent rows are data.
    """
    # Get headers from first row (or second row, depending on format)
    headers = []
    header_row = None
    
    # Try to find header row (look for common patterns)
    for row_num in range(1, min(5, sheet.max_row + 1)):
        row = [cell.value for cell in sheet[row_num]]
        # Check if this looks like a header row - FIXED LOGIC
        if any(h and isinstance(h, str) and ('Column' in h or 'Field' in h or 'Name' in h)
               for h in row):
            headers = row
            header_row = row_num
            break
    
    if not headers:
        # Fallback: use first row
        headers = [cell.value for cell in sheet[1]]
        header_row = 1
    
    # Convert data rows to dictionaries
    data = []
    for row in sheet.iter_rows(min_row=header_row + 1, values_only=True):
        if not any(row):  # Skip empty rows
            continue
        
        row_dict = {}
        for i, value in enumerate(row):
            if i < len(headers) and headers[i]:
                # Clean header name
                header = str(headers[i]).strip()
                row_dict[header] = value
        
        if row_dict:  # Only add non-empty rows
            data.append(row_dict)
    
    return data


def _extract_summary(sheet) -> Dict[str, str]:
    """Extract summary information from DGBee Summary sheet"""
    summary = {}
    
    for row in sheet.iter_rows(min_row=1, max_row=20, values_only=True):
        if row[0] and isinstance(row[0], str):
            # Look for key-value pairs
            if len(row) > 1 and row[1]:
                summary[str(row[0]).strip()] = str(row[1]).strip()
    
    return summary


def extract_entities_from_guardrails(guardrails_json_str: str) -> List[str]:
    """
    Extract entity names from Guardrails JSON string.
    Helper function for analysis.
    
    Args:
        guardrails_json_str: JSON string from convert_guardrails_to_json()
        
    Returns:
        List of entity names
    """
    data = json.loads(guardrails_json_str)
    return list(data.get("sheets", {}).keys())