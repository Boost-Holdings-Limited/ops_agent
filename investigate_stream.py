import boto3
from typing import Dict, Any
from langchain.llms.bedrock import Bedrock
from langchain_core.runnables import Runnable, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from datetime import datetime

import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import ast
from tabulate import tabulate
from dotenv import load_dotenv
import os


# Load data from CSVs
def load_data():
    cbs = pd.read_csv("data/cbs.csv")
    mpesa = pd.read_csv("data/mpesa.csv")
    switch = pd.read_csv("data/switch_logs.csv")
    disb = pd.read_csv("data/disbursement_api.csv")
    #tickets = pd.read_csv("data/ticketing_workflow.csv")
    return cbs, mpesa, switch, disb#, tickets

cbs, mpesa, switch, disb = load_data() #, tickets

# Load environment variables
load_dotenv()

# Fetch from environment
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region = os.getenv("AWS_REGION", "us-east-1")



# AWS Bedrock client
try:

    bedrock_runtime = boto3.client(
            service_name = "bedrock-runtime",
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
except Exception as e:
    st.error(f"❌ Failed to initialize AWS client: {e}")
    

# Initialize Bedrock LLM
llm = Bedrock(
    client=bedrock_runtime,
    model_id="meta.llama3-70b-instruct-v1:0",
    model_kwargs={"temperature": 0.3}
)

# Investigation Prompt Template #, "ticket" Ticketing System: {ticket}
prompt = PromptTemplate(
    input_variables=["txn_id", "cbs", "mpesa", "switch", "disb"],
    template="""
You are a banking operations assistant. A user has submitted an unmatched transaction that needs investigation.

Transaction ID: {txn_id}

CBS:
{cbs}

M-Pesa:
{mpesa}

Switch Logs:
{switch}

Disbursement API:
{disb}


CBS is the main system where the reconcilling error or was discovered. Interogate the other systems i.e. M-Pesa, Switch Logs and Disbursement API then 
return very very brief and concise results with the following format:
root_cause: Identify the likely root cause for the unmatched transaction 
confidence_score: Add a short confidence score (as a percentage) at the end, indicating how confident you are in your conclusion
next_steps:  Recommend next steps to complete the transaction
summary_report: short summary on how the transaction moved in the systems what contributed most to your conclusion
records: return all records system and the records in json wrapped in {{}}

do not output python code

"""
)

chain = RunnablePassthrough() | prompt | llm | StrOutputParser()

# Helper to get data row
def get_row(df, txn_id):
    return df[df["txn_id"] == txn_id].to_dict(orient="records")

def extract_section(label, text):
    import re
    match = re.search(rf"{label}:(.*?)(?=\n[A-Z]|$)", text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else "-"

st.set_page_config(layout="wide")
# Streamlit UI
st.markdown("### 🔎 Investigate a Transaction")

tab1, tab2 = st.tabs(["🧾 Individual Txn", "📂 Search Multiple Txn"])

with tab1:
    st.markdown("#### Investigate a Single Transaction")
    txn_id = st.text_input("Enter a Transaction ID (e.g., TXN123456):")
    # Single Txn Investigation
    if txn_id:
        results = []
        cbs_row = get_row(cbs, txn_id)
        mpesa_row = get_row(mpesa, txn_id)
        switch_row = get_row(switch, txn_id)
        disb_row = get_row(disb, txn_id)
        #ticket_row = get_row(tickets, txn_id)

        if not cbs_row:
            st.error("Transaction ID not found in CBS data.")
        else:
            try:
                output = chain.invoke({
                    "txn_id": txn_id,
                    "cbs": str(cbs_row[0]),
                    "mpesa": str(mpesa_row[0]) if mpesa_row else "N/A",
                    "switch": str(switch_row[0]) if switch_row else "N/A",
                    "disb": str(disb_row[0]) if disb_row else "N/A",
                    #"ticket": str(ticket_row[0]) if ticket_row else "N/A"
                })
                #st.text_area("🧠 AI Investigation Result", value=output, height=400)
                output = output.replace("```","")
                output = output.replace("output:","")
                output = output.replace("Here is the solution:","")  
                
                root_cause = extract_section("root_cause", output)
                confidence_score = extract_section("confidence_score", output)
                next_steps = extract_section("next_steps", output)
                summary_report = extract_section("summary_report", output)
                records = extract_section("records", output)

                results.append({
                    "txn_id": txn_id,
                    "root_cause": root_cause,
                    "confidence_score": confidence_score,
                    "next_steps": next_steps,
                    "summary_report": summary_report,
                    "records": records
                    })
                
                if results:
                    df_results = pd.DataFrame(results)

                    #st.markdown("### 🧾 Transaction Investigation Summary")
                    for _, row in df_results.iterrows():
                    
                        with st.expander(f"🧠** Details for {row['txn_id']}**"):
                                st.markdown(f"**Root Cause:** {row['root_cause']}")
                                st.markdown(f"**Confidence Level:** {row['confidence_score']}")
                                st.markdown(f"**Next Steps:** {row['next_steps']}")
                                st.markdown(f"**Summary Report:** {row['summary_report']}")
                               
                                records_str = row['records']
                                records_str = records_str.replace("nan", "None")
                            
                                records_dict = ast.literal_eval(records_str)
                                # Display each system record in its own DataFrame and section
                                combined_output = ""
                                for system, record in records_dict.items():
                                    #st.markdown(f"### 📂 {system}")
                                    df = pd.DataFrame([record])
                                    table_text = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
                                    combined_output += f"{system}\n{table_text}\n"
                                # Display all in one text area
                                st.text_area("📄 **System Evidence**", value=combined_output, height=300)
                                #st.text_area("📄 Full System Records", value=combined_output, height=600)



            except Exception as e:
                st.error("❌ Investigation failed.")
                st.exception(e)


with tab2:
    #st.markdown("#### Upload a CSV File of Transaction IDs")
    uploaded_txn_file = st.file_uploader(
        label="Upload CSV File of Transaction IDs",
        type="csv",
        label_visibility="visible",
        help="The file should have a column named 'txn_id'"
    )

    # Multiple Txns via CSV
    if uploaded_txn_file:
        results = []

        try:
            df_uploaded = pd.read_csv(uploaded_txn_file)
            if "txn_id" not in df_uploaded.columns:
                st.error("CSV must contain a column named 'txn_id'.")
            else:
                st.success(f"Processing {len(df_uploaded)} transactions...")
                for index, row in df_uploaded.iterrows():
                    txn_id = row["txn_id"]

                    cbs_row = get_row(cbs, txn_id)
                    mpesa_row = get_row(mpesa, txn_id)
                    switch_row = get_row(switch, txn_id)
                    disb_row = get_row(disb, txn_id)
                    #ticket_row = get_row(tickets, txn_id)
                    
                    
                    
                    #txn_results
                    if not cbs_row:
                        st.error("Transaction ID not found in CBS data.")
                    else:
                        try:
                            output = chain.invoke({
                                    "txn_id": txn_id,
                                    "cbs": str(cbs_row[0]),
                                    "mpesa": str(mpesa_row[0]) if mpesa_row else "N/A",
                                    "switch": str(switch_row[0]) if switch_row else "N/A",
                                    "disb": str(disb_row[0]) if disb_row else "N/A",
                                    #"ticket": str(ticket_row[0]) if ticket_row else "N/A"
                                })
                            output = output.replace("```","")
                            output = output.replace("output:","")
                            output = output.replace("Here is the solution:","")               
                
                            root_cause = extract_section("root_cause", output)
                            confidence_score = extract_section("confidence_score", output)
                            next_steps = extract_section("next_steps", output)
                            summary_report = extract_section("summary_report", output)
                            records = extract_section("records", output)

                            results.append({
                                "txn_id": txn_id,
                                "root_cause": root_cause,
                                "confidence_score": confidence_score,
                                "next_steps": next_steps,
                                "summary_report": summary_report,
                                "records": records
                            })
                           # st.text_area("🧠 AI Result", value=output, height=300)
                        except Exception as e:
                             st.error("❌ Error during processing.")
                             st.exception(e)
                
                if results:
                    df_results = pd.DataFrame(results)

                    st.markdown("### 🧾 Transaction Investigation Summary")

                    # Header
                    col1, col2, col3, col4 = st.columns([2, 3, 3, 4])
                    col1.markdown("**Transaction ID**")
                    col2.markdown("**Root Cause**")
                    col3.markdown("**Confidence Level**")
                    
                    col4.markdown("**Next Steps**")

                    # Rows with expandable reports
                    for _, row in df_results.iterrows():
                        col1, col2, col3, col4 = st.columns([2, 3, 3, 4])
                        col1.markdown(row['txn_id'])
                        col2.markdown(row['root_cause'])
                        col3.markdown(row['confidence_score'])
                        col4.markdown(row['next_steps'])

                        with st.expander(f"**🧠 Detailed Report for {row['txn_id']}**"):
                            st.markdown(f"**Summary:** {row['summary_report']}")
                            #st.markdown(f"**Next Steps:** {row['next_steps']}")
                            # Show 'records' in a multi-line, scrollable textbox
                            records_str = row['records']
                            records_str = records_str.replace("nan", "None")
                            records_dict = ast.literal_eval(records_str)
                            # Replace `nan` with a string or Python None
                            
                            # Display each system record in its own DataFrame and section
                            combined_output = ""
                            for system, record in records_dict.items():
                                #st.markdown(f"### 📂 {system}")
                                df = pd.DataFrame([record])
                                table_text = tabulate(df, headers='keys', tablefmt='grid', showindex=False)
                                combined_output += f"{system}\n{table_text}\n"
                            # Display all in one text area
                            st.text_area("📄 **System Evidence**", value=combined_output, height=300, key=row['txn_id'])


        except Exception as e:
            st.error("❌ Failed to process uploaded file.")
            st.exception(e)




