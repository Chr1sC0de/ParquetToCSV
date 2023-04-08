import io

import polars as pl
import streamlit as st


def main():
    st.title("Drag and Drop Parquet to CSV Converter")

    with st.spinner("Waiting for files to upload..."):
        uploaded_files = st.file_uploader(
            "Drop your parquet files here",
            type=["parquet"],
            accept_multiple_files=True,
        )

    if len(uploaded_files) > 0:
        uploaded_files.sort(key=lambda x: x.name)
        st.write("Sorted files")
        st.write(uploaded_files)
        # Read the uploaded Parquet file into a Pandas DataFrame
        try:
            with st.spinner("Converting files to CSV..."):
                dataframes = [
                    pl.read_parquet(
                        io.BytesIO(f.getvalue()),
                        use_pyarrow=True,
                        pyarrow_options={"coerce_int96_timestamp_unit": "ms"},
                    )
                    for f in uploaded_files
                ]
                if len(dataframes) == 1:
                    df = dataframes[0]
                else:
                    df = pl.concat(dataframes)

                def writer_method(x):
                    return x.write_csv().encode("utf-8")

                st.subheader("Dataframe preview")
                st.write(
                    "DataFrame shape %s. \
                        (if start index = end index only 1 row is shown)"
                    % (str(df.shape))
                )
                p_start = st.number_input(
                    "Preview: start index", step=1, value=0
                )
                p_end = st.number_input(
                    "Preview: end index",
                    step=1,
                    value=min(4, df.shape[0] - 1),
                )
                req_cols = st.multiselect(
                    "select the columns to preview and write to csv",
                    df.columns,
                    df.columns,
                )
                st.dataframe(
                    df[p_start : p_end + 1]
                    .select([pl.col(c) for c in req_cols])
                    .to_pandas(timestamp_as_object=True)
                )
                st.write("\n")

            st.success("File has finished converting")
            st.write("Choose a subset of rows to download")
            start = st.number_input("Starting Index", value=0, step=1)
            end = st.number_input("Ending Index", value=len(df) - 1, step=1)
            st.download_button(
                "Download CSV",
                writer_method(
                    df[start : end + 1].select([pl.col(c) for c in req_cols])
                ),
                file_name="converted.csv",
                use_container_width=True,
            )
        except Exception as e:
            st.error(e, icon="🚨")
    else:
        st.warning("Please upload a Parquet file.")


if __name__ == "__main__":
    main()
