from google.cloud import language


def analyze_sentiment(text_content):
    """
    Function that send text to google cloud language service to get it's sentiment score.
    The score can be in range from -1 to 1 (-1 most negative, 1 most positive)
    :param text_content: text that should be analysed
    :return: sentiment as decimal
    """
    # preparing sentiment analysis
    client = language.LanguageServiceClient()

    # document possible types: PLAIN_TEXT, HTML
    doc_type = language.Document.Type.PLAIN_TEXT

    doc_lang = "en"
    document = {"content": text_content, "type_": doc_type, "language": doc_lang}

    # encoding possible values: NONE, UTF8, UTF16, UTF32
    doc_encoding = language.EncodingType.UTF8

    # Sent response
    response = client.analyze_sentiment(request={'document': document, 'encoding_type': doc_encoding})

    # Return sentiment value
    return response.document_sentiment.score
