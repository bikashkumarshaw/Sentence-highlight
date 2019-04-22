from funcserver import Server
from funcserver import Client

import tornado.ioloop
import tornado.web

from kwikapi.tornado import RequestHandler
from kwikapi import API, Request
import threading
import copy
import json
import random
import logging
from urllib.parse import urljoin
#from urlparse import urljoin
import requests
import time
import re
from copy import deepcopy

from es_index import SEARCH_INDEXES
from utils import HmacAuth, PY2HmacAuth
from kwikapi.tornado import RequestHandler
from tornado.web import RequestHandler as TornadoRequestHandler

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

COUNT =0

ES_CLUSTER_HEALTH = "/_cluster/health"
SUCCESS_CODE = 200
ERROR_CODE = 500

PERCENTAGE = 65
DEFAULT_SERVER = 'corpus'

HEADERS = {'Content-type': 'application/json'}

class ElasticSearch(object):
    def __init__(self, location, index, args, log):

        self.location = location
        self.index = index
        self.args = args
        self.log = log

    def _make_search_query(self, doc_id, num, query, language, fields):
        query_dict =       {
          "highlight": {
            "fields": {
              "text": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "fvh",
                "fragment_size": self.args.fragment_size
              },
              "abstract": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "fvh",
                "fragment_size": self.args.fragment_size
              },
              "title": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "unified",
                "fragment_size": self.args.fragment_size
            }
            }
          },
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "language.name": language
                  }
                },
                {
                  "bool": {
                    "must": {
                      "range": {
                        "language.percentage": {
                          "gte": PERCENTAGE
                        }
                      }
                    }
                  }
                },
                {
                  "bool": {
                    "filter": {
                      "ids": {
                        "values": doc_id
                      }
                    },
                    "must": [
                      {
                        "query_string": {
                          "minimum_should_match": "100%",
                          "fields": fields,
                          "query": query
                        }
                      }
                    ]
                  }
                }
              ]
            }
          },
          "_source": [
            "language",
            "url"
          ],
          "timeout": "60s",
          "size": num
        }

        return query_dict

    def _make_complete_search_query(self, doc_id, num, query, language, fields):
        query_dict =       {
          "highlight": {
            "fields": {
              "text": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "fvh",
                "fragment_size": self.args.fragment_size,
                "boundary_scanner": "sentence"
              },
              "abstract": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "fvh",
                "fragment_size": self.args.fragment_size,
                "boundary_scanner": "sentence"
              },
              "title": {
                "number_of_fragments": self.args.number_of_fragments,
                "type": "unified",
                "fragment_size": self.args.fragment_size,
            }
            }
          },
          "query": {
            "bool": {
              "must": [
                {
                  "match": {
                    "language.name": language
                  }
                },
                {
                  "bool": {
                    "must": {
                      "range": {
                        "language.percentage": {
                          "gte": PERCENTAGE
                        }
                      }
                    }
                  }
                },
                {
                  "bool": {
                    "filter": {
                      "ids": {
                        "values": doc_id
                      }
                    },
                    "must": [
                      {
                        "query_string": {
                          "minimum_should_match": "100%",
                          "fields": fields,
                          "query": query
                        }
                      }
                    ]
                  }
                }
              ]
            }
          },
          "_source": [
            "language",
            "url"
          ],
          "timeout": "60s",
          "size": num
        }

        return query_dict

    def _do_search(self, req, doc_id, num, query, complete_sentence, language, fields):
        sent_obj = Sentence_highlight(self.log, self.args)
        sent_obj._update_req_logger(req, query=query)
        hdrs = sent_obj._get_headers(req)
        language = language.upper()
        u = urljoin(self.location, '/%s/%s/%s' % (self.index, "document", '_search'))
        if complete_sentence:
            query_dict = self._make_complete_search_query(doc_id, num, query, language, fields)
        else:
            query_dict = self._make_search_query(doc_id, num, query, language, fields)
        query_dict['_source'] = ['language', 'url']

        data = json.dumps(query_dict) if isinstance(query_dict, dict) else query_dict

        auth=HmacAuth(self.args.user, self.args.password)
        t1 = time.time()
        # TODO get me only required fields
        if hdrs:
            r = requests.get(url=u, data=data, headers={ **HEADERS, **hdrs }, timeout=5)
        else :
            r = requests.get(url=u, auth=auth, data=data, headers=HEADERS, timeout=5)

        t2 = time.time()
        t = (t2-t1)*1000

        data = r.json()

        return data

class Sentence_highlight(object):


    def __init__(self, log, args):

        self.COUNT = 0
        self.args = args
        self.log = log

    def _get_headers(self, req):
        if not req:
            return {}

        if 'Cookie' not in req.headers:
            return {}

        hdrs = {}

        hdrs['Cookie'] = req.headers['Cookie']

        if 'Referer' in req.headers:
            hdrs['Referer'] = req.headers['Referer']

        if 'X-Unique-ID' in req.headers:
            hdrs['X-Unique-ID'] = req.headers['X-Unique-ID']

        return hdrs

    def _update_req_logger(self, req, **kwargs):
        req.log = req.log.bind(**self._get_headers(req), **kwargs)

    def _clean_searchify(self, w: str)-> str:
        words = []
        w = w.replace('"', '')
        if "AND" in w:
            for s in w.split('AND'):
                if not s.strip():
                   continue
                s = '"%s"' % s.strip() if not "AND" in s else s
                words.append(s)

            return " AND ".join(words)
        if "OR" in w:
            for s in w.split('OR'):
                if not s.strip():
                    continue
                s = '"%s"' % s.strip() if not "OR" in s else s
                words.append(s)
            return " OR ".join(words)
        s = '%s' % w
        return s

    def _searchify(self, w: str)-> str:
        w = ' '.join(w.split('_')) if '-' in w or '_' in w else w
        if w.count('""') > 1:
	   # removing extra quotes which are given by User
           w = w.replace('""', '"')

        return w

    def searchify(self, w: str)-> str:
        words = [ self._searchify(i) for i in w.split() if i.strip() ]
        w = ' '.join(words)
        if not w.strip():
            return
        return self._clean_searchify(w).strip()

    def es_req(self, req: object, doc_ids: list, num: int, total: int, _query:str, query: str, complete_sentence: bool, language: str, fields: list, result: list)-> None:
        if doc_ids:
            source = ','.join(SEARCH_INDEXES.values())
            url = '{0}'.format(self.args.es_url)
            index = ElasticSearch(url, source, self.args, self.log)

            t1 = time.time()
            req.log.info("contacting ES")
            doc = index._do_search(req, doc_ids, num, query, complete_sentence, language, fields)
            doc["query"] = query
            doc["_query"] =_query
            t2 = time.time()
            t = (t2-t1)*1000
            self.log.info("time taken to get resp from es {}".format(t))
            self.COUNT = self.COUNT+1

            if not doc.get('hits', {}).get('hits', []):
                doc = {'hits': {'hits': [], 'total': 0},"query": query, "sentences": [], "_query": _query, "num": 0, "total": total, "scroll_id": ""}

        else:
            doc = {'hits': {'hits': [], 'total': 0},"query": query, "sentences": [], "_query": _query, "num": 0, "total": total, "scroll_id": ""}

        result.append(doc)

    def _bulk_get_matched_sentences(self, req: object, r: list, complete_sentence: bool, window_size: int, language: str, num: int, fields: list)-> list:

        search_q = []
        prep_query = time.time()
        token_pairs = []
        docs = []

        threads = []
        result = []
        skeleton = []
        for record in r:
            doc_ids = record.get("matched_doc_ids", [])
            token1 = self.searchify(record.get("token1", ""))
            token2 = self.searchify(record.get("token2", ""))
            if window_size > 0:
                total = record.get("matched_doc_count", 0)
                _query = "{0},{1}".format(token1.strip(), token2.strip())
                cleaned_tokens = [token1, token2]
                query = '+'.join(cleaned_tokens)
                query = '"'+query+'"'
                query = '%s~%d' % (query, window_size)
            else:
                total = record.get("matched_doc_count", 0)
                _query = "{0},{1}".format(token1.strip(), token2.strip())
                token1 = '"'+token1+'"'
                token2 = '"'+token2+'"'
                cleaned_tokens = [token1, token2]
                query = " AND ".join(cleaned_tokens)
            thread = threading.Thread(target=self.es_req, kwargs=dict(req=req, doc_ids=doc_ids, num=num, total=total, _query=_query, query=query, complete_sentence=complete_sentence, language=language, result=result, fields=fields))
            threads.append(thread)

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        return result

    def _bulk_get_doc_cooccurrences(self, req, chunk: list, url: str, num: int, is_random: bool, result: list)-> list:
        auth = PY2HmacAuth(self.args.user, self.args.password)
        SERVER_OBJ = Client(url, auth=auth)
        SERVER_OBJ.set_batch()
        self.log.info('Contacting the server for bulk_get_doc_cooccurrences: %s' % (SERVER_OBJ.server_url))
        for pair in chunk:
            #tokens = "{0},{1}".format(pair[0].strip(), pair[1].strip())
            token1, token2 = pair
            token1 = token1.strip()
            token2 = token2.strip()

            SERVER_OBJ.get_doc_ids(token1=token1, token2=token2, num=num, is_random=is_random)

        _r = SERVER_OBJ.execute()
        if not isinstance(_r, list):
            _r = [ _r ]

        SERVER_OBJ.unset_batch()
        result.extend(_r)
        return result

    def bulk_get_matched_sentences(self, req: Request, tokens: list, fields: list=["title", "text", "abstract"], source: str=DEFAULT_SERVER, complete_sentence: bool=False, window_size: int=0, language: str="ENGLISH", num: int=150, is_random: bool=False)-> list:

        st = time.time()
        '''
        @tokens: (['a', 'x'], ['b', 'x'], ['c', 'x'])
        '''

        req.log.info("get_matched_sentences_called")

        self._update_req_logger(req, tokens=tokens)

        if isinstance(fields, str):
            fields = fields.lower()
            fields = fields.split(",")

        if not isinstance(is_random, bool):
            is_random = False if is_random.lower()=="false" else True

        calls = self.args.doc_cooccur_replicas_count
        chunked_toks = self.split_seq(tokens, calls)
        chunked_toks = filter(None, chunked_toks)

        result = []

        threads = []

        req.log.info("Parallel calls made to document cooccurrence")
        for chunk in chunked_toks:
            thread = threading.Thread(target=self._bulk_get_doc_cooccurrences, kwargs=dict(req, chunk=chunk, url=self.args.doc_cooccur_url, num=num, is_random=is_random, result=result))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        self.log.info("length of doccooccur result {}".format(len(result)))

        end = (time.time()-st)*1000
        self.log.info("time taken from doc cooccur service{}".format(end))

        docs = self._bulk_get_matched_sentences(req, result, complete_sentence, window_size, language, num, fields)

        self.log.info("calls made to es {}".format(self.COUNT))
        self.COUNT = 0

        _docs = []
        for token_pair in docs:
            if "_shards" not in token_pair:
                token_pair.pop('hits', {})
                _docs.append(token_pair)

            elif token_pair.get("hits", {}).get("hits", []):
                _docs.append(self._get_docs(token_pair, fields))
        auth = PY2HmacAuth(self.args.user, self.args.password)
        SERVER_OBJ = Client(self.args.doc_cooccur_url, auth=auth)
        pairs  = SERVER_OBJ.bulk_get_tokens(tokens)

        new_tokens = []
        for t in pairs:
            t1,t2 = t
            t1,t2 = self.searchify(t1), self.searchify(t2)
            new_tokens.append([t1,t2])

        final_docs = []
        sync_time = time.time()
        for token_pair in new_tokens:
            for doc in _docs:
                if doc.get("_query", "").split(",") == token_pair:
                    final_docs.append(doc)
                    _docs.remove(doc)
                    break
                else:
                    continue
        self.log.info("time taken to sync the docs {0}".format((time.time()-sync_time)*1000))

        self._update_req_logger(req)

        return final_docs

    def split_seq(self, seq: list, size: int)-> list:
        newseq = []
        split_seq_size = (1.0 / size) * len(seq)
        for i in range(size):
            newseq.append(seq[int(round(i * split_seq_size)):int(round(( i + 1 ) * split_seq_size))])
        return newseq


    def _get_docs(self, docs: dict, fields: list)-> dict:
        pos = 0
        while(pos<len(docs.get("hits", {}).get("hits", []))):
            doc = docs.get("hits", {}).get("hits", [])[pos]
            _source = doc.pop("_source")
            if not doc.get("highlight", []):
                docs.get("hits", {}).get("hits", []).pop(pos)
                continue
            highlight = doc.pop("highlight")
            doc["language"] = _source.get("language", {}).get("name", "")
            doc["url"] = _source.get("url", "")
            if "title" in fields and not "text" in fields:
                doc["sentences"] = highlight.get("title", [])
            else:
                doc["sentences"] = highlight.get("text", [])
            doc.pop("_type")
            doc.pop("_score")
            pos = pos+1
        hits = docs.pop("hits")
        docs.pop("_shards")
        docs.pop("took")
        docs.pop("timed_out")
        docs["sentences"] = hits.pop("hits")
        return docs

    def get_matched_sentences(self, req: Request, tokens: str='', fields: list=["text", "title", "abstract"], source: str=DEFAULT_SERVER, num: int=150, page: int=1, complete_sentence: bool=False, window_size: int=0, language: str="english", is_random: bool=False)-> dict:


        self._update_req_logger(req, tokens=tokens)
        req.log.info("get_matched_sentences_called")

        if "," in tokens:
            tokens = tokens.split(",")
            token1 = tokens[0]
            token2 = tokens[1]
        else:
            token1 = tokens
            token2 = ''

        hdrs = self._get_headers(req)
        url = self.args.doc_cooccur_url
        auth = PY2HmacAuth(self.args.user, self.args.password)

        SERVER_OBJ = Client(url, auth=auth)
        SERVER_OBJ.set_batch()
        if isinstance(fields, str):
            fields = fields.lower()
            fields = fields.split(",")

        if not isinstance(is_random, bool):
            is_random = False if is_random.lower() == "false" else True

        if token1 and token2:
            req.log.info("contacting document cooccurrence service with the tokens:{0}, {1}".format(token1, token2))
            t1 = time.time()
            SERVER_OBJ.get_doc_ids(token1=token1, token2=token2, num=num, page=page, is_random=is_random)
            _r = SERVER_OBJ.execute()
            SERVER_OBJ.unset_batch()
            t2 = time.time()
            t = (t2-t1)*1000
            self.log.info("time taken to request get cooccurence {}".format(t))
            r = _r
            token1 = self.searchify(token1)
            token2 = self.searchify(token2)
            if window_size >0:
                cleaned_tokens = [token1, token2]
                query = '+'.join(cleaned_tokens)
                query = '"'+query+'"'
                query = '%s~%d' % (query, window_size)
            else:
                token1 = '"'+token1+'"'
                token2 = '"'+token2+'"'
                cleaned_tokens = [token1, token2]
                query = " AND ".join(cleaned_tokens)
        else:
            req.log.info("contacting document cooccurrence service with the token:{}".format(token1))
            t1 = time.time()
            SERVER_OBJ.get_doc_ids(token1=token1, num=num, page=page, is_random=is_random)
            _r = SERVER_OBJ.execute()
            SERVER_OBJ.unset_batch()
            t2 = time.time()
            t = (t2-t1)*1000
            self.log.info("time taken to request get cooccurence {}".format(t))
            r = _r
            token1 = self.searchify(token1)
            query = token1

        resp = r[0]

        doc_id = resp.get("matched_doc_ids", [])

        source = ','.join(SEARCH_INDEXES.values())

        url = self.args.es_url
        index = ElasticSearch(url, source, self.args, self.log)
        es_time = time.time()
        req.log.info("contacting ES")
        docs = index._do_search(req, doc_id, num, query, complete_sentence, language, fields)
        self.log.info("time taken from es {}".format((time.time()-es_time)*1000))

        prep_sent_st = time.time()
        self._get_docs(docs, fields)
        prep_sent_et = (time.time()-prep_sent_st)*1000
        self.log.info("time taken to prepare sentences {}".format(prep_sent_et))

        docs['total'] = resp.get("matched_doc_count", 0)
        docs['_query'] = query
        if token1 and token2:
            docs['query'] = ",".join(cleaned_tokens)
        else:
            docs['query'] = token1
        docs['scroll_id'] = ""
        docs["num"] = len(docs.get("sentences", []))

        self._update_req_logger(req)

        return docs

class HealthCheck(TornadoRequestHandler):
    def initialize(self, args):
        self.args = args

    def check_status(self):
        url = self.args.es_url
        es_health_url = urljoin(url, ES_CLUSTER_HEALTH)
        auth=HmacAuth(self.args.user, self.args.password)
        response = requests.get(es_health_url, auth=auth).status_code
        if response == SUCCESS_CODE:
            self.set_status(response)
        else:
            self.set_status(ERROR_CODE)

    get = post = check_status

class SentenceAPI(Server):

    def define_args(self, parser):
        super(SentenceAPI, self).define_args(parser)

        parser.add_argument('--es-url', type=str, help='Elasticsearch URL')
        parser.add_argument('--number-of-fragments', type=int, default=100, help='ES Number of Fragments default to 100')
        parser.add_argument('--fragment-size', type=int, default=256, help='ES Fragment size default to 256')
        parser.add_argument('--user', type=str, help='please provide the user name for the nference setup')
        parser.add_argument('--password', type=str, help='Please provide the password for the setup')
        parser.add_argument('--doc-cooccur-url', type=str, help='Please provide the doc cooccur master url')
        parser.add_argument('--doc-cooccur-replicas-count', type=int, help='Please provide the number of replicas for doc cooccur')

    def init_app(self):
         api = API(log=self.log)
         api.register(Sentence_highlight(self.log, self.args), 'Sentence_highlight', 'v1')
         return tornado.web.Application([
	        (r'^/api/.*', RequestHandler, dict(api=api, log=self.log)),
                (r'^/healthcheck', HealthCheck, dict(args=self.args)),
	 ])
    def run(self):
        # Initiating the service
        app = self.init_app()
        app.listen(self.args.port)
        tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    SentenceAPI().start()
