import streamlit as st
import os
from PIL import Image
from src import *

def process(pdf_path, destination_name):
    # Placeholder for the processing logic that uses functions from the src module
    file_to_excel(pdf_path, f"Output/{destination_name}.xlsx")
    return f"Output/{destination_name}.xlsx"

# Streamlit app
st.set_page_config(page_title="PDF to Excel Converter", page_icon="Images\BNP_to_Excel.png")

# Streamlit app
col1, col2 = st.columns([1, 8])
with col1:
    logo = Image.open("Images\BNP_to_Excel.png")
    st.image(logo, use_column_width=True)
with col2:
    st.title('BNP statement to Excel')

# Upload PDF file
uploaded_file = st.file_uploader("Upload your BNP PDF monthly statement", type=["pdf"])

# Input for destination file name
destination_name = st.text_input("Destination file name")

# Button to run the process
if st.button("Run"):
    if uploaded_file is not None and destination_name:
        # Save the uploaded PDF to a temporary location
        temp_pdf_path = os.path.join("Data", uploaded_file.name)
        
        # Run the process function with the PDF path and destination name
        with st.spinner('Processing...'):
            excel_file_path = process(temp_pdf_path, destination_name)
        
        st.success("Processing complete!")
        # Clean up the temporary file if necessary
        #os.remove(temp_pdf_path)

                # Display a download button for the generated Excel file
        with open(excel_file_path, "rb") as f:
            btn = st.download_button(
                label="Download Excel file",
                data=f,
                file_name=os.path.basename(excel_file_path),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        # Display the contents of the Excel file
        df = pd.read_excel(excel_file_path)
        st.dataframe(df)

    else:
        st.warning("Please upload a PDF file and provide a destination file name.")

