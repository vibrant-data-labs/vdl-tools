import six
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account
import vdl_tools.shared_tools.common_functions as cf


def translate_text(target, text, print=False, cred="google-credentials.json"):
    """Translates text into the target language.
    Target must be an ISO 639-1 language code.
    See https://g.co/cloud/translate/v2/translate-reference#supported_languages
    """
    # get google cloud credentials see: https://cloud.google.com/docs/authentication/getting-started
    credentials = service_account.Credentials.from_service_account_file(cred)
    translate_client = translate.Client(credentials=credentials)

    if isinstance(text, six.binary_type):
        text = text.decode("utf-8")

    # Text can also be a sequence of strings, in which this will return a sequence of results for each text.
    result = translate_client.translate(text, target_language=target)

    if print:
        print("Text: {}".format(result["input"]))
        print("Translation: {}".format(result["translatedText"]))
        print("Detected source language: {}".format(result["detectedSourceLanguage"]))

    # translated = result["translatedText"]
    # source = result["detectedSourceLanguage"]

    return result


def translate_cols(df, textcols, outfile, target="en", lang_detCol="language"):
    """
    Translate text of a list of string columns
    First check to detect language.
    If it's not the target language or empty, then translate and replace with target language
    ----------
    textcols : list of string cols
    target : target language ('en' default)
    Returns: df with cols in foreign language and col in target language
    """
    for col in textcols:
        print("translating %s to english" % col)
        df.loc[:, col + "_en"] = df.apply(
            lambda x: translate_text(target, x[col])["translatedText"]
            if x[lang_detCol] != "en"
            else x[col],
            axis=1,
        )
        # df[col+"_en"] = df[col+"_en"].apply(lambda x: "" if x == 'in' else x) # remove empty results from google translate
    cf.write_excel_no_hyper(df, outfile)
    return df
