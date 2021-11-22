from google.cloud import language


def analyze_sentiment(text_content):
    # preparing sentiment analysis
    client = language.LanguageServiceClient()

    # document possible types: PLAIN_TEXT, HTML
    doc_type = language.Document.Type.PLAIN_TEXT

    doc_lang = "en"
    document = {"content": text_content, "type_": doc_type, "language": doc_lang}

    # encoding possible values: NONE, UTF8, UTF16, UTF32
    doc_encoding = language.EncodingType.UTF8

    response = client.analyze_sentiment(request={'document': document, 'encoding_type': doc_encoding})

    return response.document_sentiment.score
