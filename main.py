from functions.functions_main import main, debug

if __name__ == "__main__":
    DEBUG = False
    if DEBUG:
        debug(export_path="C:/Users/scc/Desktop/mail-filter/fertig/email_verarbeitet.xlsx",
              files_path="C:/Users/scc/Desktop/mail-filter/unverarbeitet/")
    else:
        main(export_path="C:/Users/scc/Desktop/mail-filter/fertig/email_verarbeitet.xlsx",
             files_path="C:/Users/scc/Desktop/mail-filter/unverarbeitet/",
             failsafe_dir="C:/Users/scc/Desktop/mail-filter/unverarbeitet/failsafe/")
