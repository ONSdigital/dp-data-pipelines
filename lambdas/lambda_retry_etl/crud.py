def get_all_documents_list(collection):
    return [document for document in collection.find()]
