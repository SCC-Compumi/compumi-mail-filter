from functions.prepare_imports import *


def get_files(files_path: str) -> list[list[str]]:
    files = []
    files_old = []
    counter = 0

    for file in os.listdir(files_path):
        if Path(os.path.join(files_path, file)).is_dir():
            continue
        if file.endswith(".csv"):
            files.append(str(os.path.join(files_path, file)))
        else:
            counter += 1
            file_path = os.path.join(files_path, file)
            file_renamed = f"list-{counter}.csv"
            file_path_renamed = os.path.join(files_path, file_renamed)
            os.rename(file_path, file_path_renamed)
            files.append(str(file_path_renamed))
            files_old.append(str(file))

    if not files:
        raise FileNotFoundError("No file in \"files_path\"")

    return [files, files_old]


def set_vars() -> list[list[str] | dict[str, list[Any]]]:
    rgx_id = r"\b\w{10}\b"
    rgx_email_from = r"from=<[^@]+@[^@]+\.[^>]+>"
    rgx_email_to = r"to=<[^@]+@[^@]+\.[^>]+>"

    rgx_list = [rgx_id, rgx_email_from, rgx_email_to]

    result_dict = {
        "filename": [],
        "message_id": [],
        "from": [],
        "to": []
    }

    return [rgx_list, result_dict]


def import_and_reformat_df(file: str) -> list[pd.DataFrame | Any]:
    df = pd.read_csv(file, sep="\t", encoding="latin1", dtype=str, lineterminator="\n")
    temp_df = pd.DataFrame(columns=["Mails"], dtype=str)
    temp_df[["Mails"]] = df.values.tolist()
    temp_df.dropna().reset_index().astype(str)

    df_filtered = temp_df[temp_df["Mails"].str.contains("dsn=")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("dsn=2.0.0")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("dsn=2.6.0")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("dsn=2.1.5")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("dsn=4.7.1")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("hinweisgeberschutz@hdgg.de")]
    df_filtered = df_filtered[~df_filtered["Mails"].str.contains("outlook.st-eli.net")]
    df_list = [temp_df, df_filtered]

    if not df_list:
        raise ValueError("Error creating pandas DataFrames")

    return df_list


def create_df(df_list: list[pd.DataFrame, Any], rgx_list: list[str], result_dict: dict[str, list[Any]],
              file_name: str) -> pd.DataFrame:
    rgx_id, rgx_email_from, rgx_email_to = rgx_list
    unchanged_df, filtered_df = df_list

    extracted_ids = []

    for index, row in filtered_df.iterrows():
        match_id = re.search(rgx_id, row["Mails"])
        if match_id:
            extracted_ids.append(match_id.group())

    extracted_ids = list(set(extracted_ids))

    df_temp_filtered = unchanged_df[unchanged_df["Mails"].apply(lambda x: any(ext_id in x for ext_id in extracted_ids))]

    for message_id in extracted_ids:
        emails_from = []
        to_row = []

        for index, row in df_temp_filtered.iterrows():
            if message_id in row["Mails"]:
                match_email_from = re.search(rgx_email_from, row["Mails"])
                match_email_to = re.search(rgx_email_to, row["Mails"])
                if match_email_from and not match_email_to:
                    temp_mail = match_email_from.group()
                    if temp_mail not in emails_from and "hinweisgeberschutz@hdgg.de" not in temp_mail:
                        emails_from.append(match_email_from.group())
                if match_email_to and not match_email_from:
                    if all(dsn not in row["Mails"] for dsn in ["dsn=2.6.0", "dsn=2.0.0", "dsn=2.1.5", "dsn=4.7.1"]):
                        if "outlook.st-eli.net" not in row["Mails"]:
                            to_row.append(str(row["Mails"]))

        result_dict["filename"].append(file_name)
        result_dict["message_id"].append(message_id)
        result_dict["from"].append("; ".join(emails_from) if emails_from else "")
        result_dict["to"].append("; ".join(to_row) if to_row else "")

    df_result = pd.DataFrame(result_dict, columns=["filename", "message_id", "from", "to"])

    return df_result


def export_df(changed_df: pd.DataFrame, export_path: str) -> None:
    writer = pd.ExcelWriter(export_path, engine='xlsxwriter')
    changed_df.to_excel(writer, sheet_name="E-Mails", index=False, na_rep="NaN")

    for column in changed_df:
        column_length = max(changed_df[column].astype(str).map(len).max(), len(column))
        col_idx = changed_df.columns.get_loc(column)
        writer.sheets["E-Mails"].set_column(col_idx, col_idx, column_length)

    writer._save()


def clean_and_copy(export_path: str, files_path: str, failsafe_dir: str) -> None:
    for file in os.listdir(files_path):
        file_old_path = os.path.join(files_path, file)
        if os.path.isfile(file_old_path):
            os.remove(file_old_path)

    for file in os.listdir(failsafe_dir):
        source_file = os.path.join(failsafe_dir, file)
        target_file = os.path.join(files_path, file)
        if os.path.isfile(source_file):
            shutil.copy2(source_file, target_file)

    if os.path.exists(export_path):
        os.remove(export_path)


def main(export_path: str, files_path: str, failsafe_dir: str) -> None:
    clean_and_copy(export_path, files_path, failsafe_dir)
    files, files_old = get_files(files_path=files_path)
    rgx_list, result_dict = set_vars()
    final_df = pd.DataFrame(columns=["filename", "message_id", "from", "to"])

    for file, file_old in zip(files, files_old):
        file_name_old = os.path.basename(file_old)
        df_list = import_and_reformat_df(file=file)
        changed_df = create_df(df_list=df_list, rgx_list=rgx_list, result_dict=result_dict, file_name=file_name_old)
        final_df = pd.concat([final_df, changed_df])

    final_df.drop_duplicates(inplace=True)

    export_df(changed_df=final_df, export_path=export_path)


def debug(export_path: str, files_path: str):
    return
