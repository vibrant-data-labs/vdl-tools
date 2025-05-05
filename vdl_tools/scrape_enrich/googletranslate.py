#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat May 22 09:27:17 2021

@author: ericberlow
"""

"""
# one way to set google cloud credentials using os.environment
# but we also did it below in the 'translate_text' function
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "<path to json cred file>"

poetry add google-cloud-translate==2.0.1
poetry add --upgrade google-api-python-client google-auth-httplib2 google-auth-oauthlib
"""

import six
from google.cloud import translate_v2 as translate
from google.oauth2 import service_account


def translate_text(
        text,
        target="en",
        print=False,
        cred="<path to json cred file",
):
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


def translate_cols(df, textcols, target="en"):
    """
    Translate text of a list of string columns
    First check to detect language.
    If it's not the target language or empty, then translate and replace with target language
    ----------
    textcols : list of string cols
    target : target language ('en' default)
    Returns: df with cols in foreign language replaced by target language
    """
    for col in textcols:
        print("translating %s to english" % col)
        df[col] = df[col].fillna("")
        # df[col] = df[col].astype(str)
        # translate text if it's not empty
        df[col + "_en"] = df.apply(
            lambda x: x[col]
            if x[col] == ""
            else translate_text(x[col], target=target)["translatedText"],
            axis=1,
        )
        df[col + "_en"] = df[col + "_en"].apply(
            lambda x: "" if x == "in" else x
        )  # remove empty results from google translate
    return df


def translate_cols_detect_source(df, textcols, target="en"):
    """
    Translate text of a list of string columns
    First check to detect language.
    If it's not the target language or empty, then translate and replace with target language
    ----------
    textcols : list of string cols
    target : target language ('en' default)
    Returns: df with cols in foreign language replaced by target language
    """
    for col in textcols:
        print("translating %s to english" % col)
        df[col] = df[col].astype(str)
        df[col] = df[col].fillna("")
        # detect source languate if it's not empty
        df["lang"] = df[col].apply(
            lambda x: ""
            if x == ""
            else translate_text(target, x)["detectedSourceLanguage"]
        )
        # translate text if it's not english (and not empty)
        df[col] = df.apply(
            lambda x: x[col]
            if (x["lang"] == target or x["lang"] == "")
            else translate_text(target, x[col])["translatedText"],
            axis=1,
        )
        df.drop(["lang"], inplace=True, axis=1)

    return df
