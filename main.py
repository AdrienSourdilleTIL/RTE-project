from api_call import fetch_data, extract_values_to_dataframe
from data_ingestion import upload_dataframe_to_snowflake
from chart_generator import generate_chart

def main():
    print("Fetching data from API...")
    data = fetch_data()
    
    print("Extracting values and converting to DataFrame...")
    df = extract_values_to_dataframe(data)
    
    if df is not None:
        print("DataFrame created successfully.")
        print(df.tail())  # Display the last few rows of the DataFrame
        
        print("Uploading DataFrame to Snowflake...")
        upload_dataframe_to_snowflake(df)
        
        print("Generating and saving chart...")
        generate_chart()
    else:
        print("Failed to create DataFrame.")

if __name__ == "__main__":
    main()
