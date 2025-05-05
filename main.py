from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse
import pandas as pd
import io
from io import BytesIO

app = FastAPI()

# Home route that serves the HTML file for file upload
@app.get("/", response_class=HTMLResponse)
async def get_upload_form():
    try: 
        with open("index.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Upload Form Not Found</h1><p>Please ensure the index.html file is present in the same directory.</p>"

# Upload route to handle the file upload and processing
@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    # Check if the file is valid
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    # Read the uploaded CSV file into pandas
    contents = await file.read()  # Read the content of the uploaded file (in bytes)
    data = pd.read_csv(io.BytesIO(contents))

    # Perform necessary transformations on the dataframe
    columns_to_remove = [
        'Email', 'Paid at', 'Fulfillment Status', 'Fulfilled at', 'Accepts Marketing', 'Currency', 'Taxes', 'Discount Code', 
        'Discount Amount', 'Shipping Method', 'Created at', 'Lineitem quantity', 'Lineitem name', 'Lineitem price',
        'Lineitem compare at price', 'Lineitem sku', 'Lineitem requires shipping', 'Lineitem taxable',
        'Lineitem fulfillment status', 'Billing Name', 'Billing Street', 'Billing Address1', 'Billing Address2', 'Billing Company',
        'Billing City', 'Billing Zip', 'Billing Province', 'Billing Country', 'Billing Phone',
        'Shipping Address1', 'Shipping Address2', 'Shipping Company', 'Shipping City', 'Shipping Zip', 'Shipping Province',
        'Shipping Country', 'Note Attributes', 'Cancelled at', 'Payment Reference', 'Refunded Amount', 'Employee',
        'Location', 'Device ID', 'Id', 'Tags', 'Risk Level', 'Source', 'Lineitem discount', 'Tax 1 Name', 'Tax 1 Value', 
        'Tax 2 Name', 'Tax 2 Value', 'Tax 3 Name', 'Tax 3 Value', 'Tax 4 Name', 'Tax 4 Value', 'Tax 5 Name', 'Tax 5 Value',
        'Phone', 'Receipt Number', 'Duties', 'Billing Province Name', 'Shipping Province Name', 'Payment ID',
        'Payment Terms Name', 'Next Payment Due At', 'Payment References', 'Outstanding Balance'
    ]
    data_cleaned = data.drop(columns=columns_to_remove)

    # Replace NaN with zeros and ensure no infinite values
    data_cleaned[['Subtotal', 'Shipping', 'Total']] = data_cleaned[['Subtotal', 'Shipping', 'Total']].fillna(0).replace([float('inf'), float('-inf')], 0)

    # Convert to absolute values and round off to the nearest integer
    data_cleaned[['Subtotal', 'Shipping', 'Total']] = data_cleaned[['Subtotal', 'Shipping', 'Total']].abs().round().astype(int)

    # Convert the "Shipping Phone" to standard Bangladesh format
    data_cleaned['Shipping Phone'] = data_cleaned['Shipping Phone'].apply(
        lambda x: f"01{str(int(x))[-9:]}" if pd.notnull(x) and isinstance(x, (int, float)) and len(str(int(x))) >= 10 else x
    )

    # Add new columns with empty values
    data_cleaned['Referred By'] = ''
    data_cleaned['Website Status'] = ''
    data_cleaned['Assigning Agent'] = ''
    data_cleaned['Discount'] = ''
    data_cleaned['Store Order ID'] = ''

    # Rearrange the columns
    columns_order = [
        'Name', 'Store Order ID', 'Vendor', 'Shipping Name', 'Shipping Phone', 'Shipping Street',
        'Payment Method', 'Referred By', 'Website Status', 'Assigning Agent', 'Notes',
        'Subtotal', 'Shipping', 'Discount', 'Total'
    ]
    data_cleaned = data_cleaned[columns_order]

    # Convert the DataFrame to a CSV file in memory
    csv = data_cleaned.to_csv(index=False)

    # Convert CSV to BytesIO (in-memory file)
    byte_io = BytesIO()
    byte_io.write(csv.encode())
    byte_io.seek(0)  # Go to the beginning of the BytesIO object

    # Return the CSV file as a downloadable attachment using StreamingResponse
    return StreamingResponse(byte_io, media_type='text/csv', headers={"Content-Disposition": "attachment; filename=cleaned_data.csv"})
