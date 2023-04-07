import io
import polars as pl
import streamlit as st

def main():
    st.title("Drag and Drop Parquet to CSV Converter")

    with st.spinner("Waiting for files to upload..."):
        uploaded_files = st.file_uploader("Drop your parquet files here", type=["parquet"], accept_multiple_files=True)

    if len(uploaded_files) > 0:
        uploaded_files.sort(key=lambda x:x.name)
        st.write("Sorted files")
        st.write(uploaded_files)
        # Read the uploaded Parquet file into a Pandas DataFrame

        with st.spinner("Converting files to CSV..."):
            df = pl.concat([pl.read_parquet(io.BytesIO(f.getvalue())) for f in uploaded_files])
            output = df.write_csv().encode("utf-8")
            st.subheader("Dataframe preview")
            st.write(df.limit(5) )
            st.write("\n")

        st.success("File has finished converting")
        st.download_button('Download CSV', output, file_name='converted.csv', use_container_width = True)
        pass
    else:
        st.warning("Please upload a Parquet file.")

if __name__ == "__main__":
    main()