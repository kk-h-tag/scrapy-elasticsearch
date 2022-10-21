#default JSON Code for ELASTIC SEARCH

search_type = '''
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "rule": "%s"
                                }
                            }
                        ]
                    }
                }
            }
            '''
search_data = '''
            {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "rule": "%s"
                                }
                            },
                            {
                                "match": {
                                    "url": "%s"
                                }
                            }
                        ]
                    }
                }
            }
            '''
insert = {'rule':'rule', 'url': 'url'}
