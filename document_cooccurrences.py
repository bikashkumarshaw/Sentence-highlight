import os
import re
import json
import time

import numpy as np
from deeputil import xcode
from numba import jit

from tornado.web import RequestHandler
from tornado.httpclient import AsyncHTTPClient
from tornado import gen

from funcserver import Server
from diskarray import DiskArray
from diskdict import DiskDict

DEFAULT_PAGE_NUMBER = 1
DEFAULT_NUM_RESULTS = 100
SUCCESS_CODE = 200
ERROR_CODE = 500
HEALTH_CHECK_URL = "http://localhost:%s/rpc/json?fn=%s&token=%s"

@jit
def _intersect(docs1, docs2):
    i = j = k = 0
    indices = np.empty_like(docs1)

    while i < docs1.size and j < docs2.size:
        if docs1[i] == docs2[j]:  # the 99% case
            indices[k] = i
            k += 1
            i += 1
            j += 1
        elif docs1[i] < docs2[j]:
            i += 1
        else:
            j += 1

    return indices[:k]

class CooccurrenceServerAPI(object):

    COOCCUR_DATA = 'cooccur.data'
    INDICES_DATA = 'indices.data'
    VOCAB = 'vocab.txt'
    IDS = 'ids.dict'

    def __init__(self, dir_path, log, args):

        O = lambda x : os.path.join(dir_path, x)

        self.log = log
        self.args = args
        self._data = DiskArray(O(self.COOCCUR_DATA), dtype = np.uint32, mode = 'r')
        self._indices = DiskArray(O(self.INDICES_DATA), dtype = np.uint64, mode = 'r')
        self._prepare_vocab(O(self.VOCAB))
        self._ids = DiskDict(O(self.IDS))

    def _prepare_vocab(self, vocab):

        #Loading the vocab file for getting the word index

        self.log.info('Loading the Vocab file into the RAM')
        self._vocab = dict()
        for index, line in enumerate(open(vocab)):
            word = line.strip().split()[0].strip()
            self._vocab[word] = index
        self.log.info('Loading is done into the RAM')

    def get_num_docs(self, token):
        token = xcode(token) if isinstance(token, unicode) else token
        token = token.lower().strip()

        resp = dict(token=token)
        index = self._vocab.get(token)

        if index is None:
            resp.update(occurrences=0)
            return resp

        doc_size = len(self._get_docs(index))
        resp.update(occurrences=doc_size)

        return resp

    def _get_docs(self, token_index):

        #Getting the documents count from the docs array with the token_index

        if token_index == (len(self._indices[:]) - 1):
            docs = self._data[self._indices[token_index]:len(self._data[:])]
        else:
            token_start = self._indices[token_index]
            token_end = token_start + \
                (self._indices[token_index + 1] - token_start)
            docs = self._data[token_start:token_end]

        return docs

    def get_cooccurrences_for_token_collection(self, tokens, control_col):

        if isinstance(tokens, basestring):
            tokens = [t.strip() for t in tokens.split(',') if t.strip()]

        if isinstance(control_col, basestring):
            control_col = [t.strip() for t in control_col.split(',') if t.strip()]

        documents = []
        for token in tokens:
            index = self._vocab.get(token, 0)
            if index == 0:
                continue
            docs =  self._get_docs(index)
            if not len(documents):
                documents = docs
                continue
            intersected = _intersect(documents, docs)
            documents = np.take(documents, intersected)

            if not len(documents):
                return {}

        if not len(documents):
            return {}

        intersect_dict = {}
        for tok in control_col:
            _index = self._vocab.get(tok, 0)
            if _index == 0:
                continue
            doc_ids =  self._get_docs(_index)
            intersect = _intersect(documents, doc_ids)
            intersect = np.take(documents, intersect)
            count = len(intersect)
            intersect_dict[tok] = count
        return intersect_dict

    def get_cooccurrences(self, token1, token2):
        token1 = xcode(token1) if isinstance(
            token1, unicode) else token1
        token1 = token1.lower().strip()

        token2 = xcode(token2) if isinstance(
            token2, unicode) else token2
        token2 = token2.lower().strip()

        resp = dict(token1=token1, token2=token2)

        index1 = self._vocab.get(token1)
        index2 = self._vocab.get(token2)

        if (index1 is None) or (index2 is None):
            resp.update(coccurrences=0)
            return resp

        docs1 = self._get_docs(index1)
        docs2 = self._get_docs(index2)

        docs = np.intersect1d(docs1, docs2)
        resp.update(cooccurrences=len(docs))

        return resp

    def get_doc_ids(self, token1='', token2='',
            num=DEFAULT_NUM_RESULTS,  is_random=False, page=DEFAULT_PAGE_NUMBER):

        token1 = xcode(token1) if isinstance(
            token1, unicode) else token1
        token1 = token1.lower()

        token2 = xcode(token2) if isinstance(
            token2, unicode) else token2
        token2 = token2.lower()

        if isinstance(is_random, basestring):
            is_random = True if is_random.lower() == "true" else False

        if token1 and not token2:
            resp = dict(token1=token1)

            index = self._vocab.get(token1)

            if index is None:
                resp["matched_doc_count"] = 0
                resp["matched_doc_ids"] = []
                resp["page"] = 1
                return resp

            t1 = time.time()
            doc_indices = self._get_docs(index)
            offset = 0

            resp["matched_doc_count"] = len(doc_indices)

            if num and is_random:
                if num > len(doc_indices):
                    doc_indices = doc_indices
                else:
                    doc_indices = np.random.choice(doc_indices, num, replace=False)

                doc_indices = doc_indices.tolist()
                doc_indices = map(int, doc_indices)

            elif num:
                offset = num
                offset = (page-1)*offset
                doc_indices = doc_indices[offset:offset+num]
                doc_indices = doc_indices.tolist()
                doc_indices = map(int, doc_indices)

            t2 = time.time()
            t= (t2-t1)*1000
            self.log.info("time taken to get docs {}".format(t))

            doc_ids = []
            for id_ in doc_indices:
                _id = self._ids[id_]
                doc_ids.append(_id)
            resp["matched_doc_ids"] = doc_ids
            resp["page"] = page
            return resp
        else:
            resp = dict(token1=token1,token2=token2)
            vocab_map_st = time.time()
            index1 = self._vocab.get(token1)
            index2 = self._vocab.get(token2)
            vocab_map_et = (time.time()-vocab_map_st)*1000
            self.log.info("time taken to get index from vocab {}".format(vocab_map_et))

            if (index1 is None) or (index2 is None):
                resp["matched_doc_count"] = 0
                resp["matched_doc_ids"] = []
                resp["page"] = 1
                return resp

            doc_st = time.time()
            docs1 = self._get_docs(index1)
            docs2 = self._get_docs(index2)
            doc_et = (time.time() - doc_st)*1000
            self.log.info("time taken to get doc ids {}".format(doc_et))

            docs_time = time.time()
            doc_indeces = _intersect(docs1, docs2)
            resp["matched_doc_count"] = len(doc_indeces)
            doc_indeces = np.take(docs1, doc_indeces)
            docs_et = time.time()
            final_docs_time = (docs_et-docs_time)*1000
            self.log.info("time taken to intersect {}".format(final_docs_time))
            offset = 0

            if num and is_random:
                if num > len(doc_indeces):
                    doc_indeces = doc_indeces
                else:
                    doc_indeces = np.random.choice(doc_indeces, num, replace=False)

                doc_indeces = doc_indeces.tolist()
                doc_indeces = map(int, doc_indeces)

            elif num:
                offset = num
                offset = (page-1)*offset
                doc_indeces = doc_indeces[offset:(offset+num)]
                doc_indeces = doc_indeces.tolist()
                doc_indeces = map(int, doc_indeces)

            doc_ids = [self._ids[i] for i in doc_indeces]
            resp["matched_doc_ids"] = doc_ids
            resp["page"] = page
            return resp

    def split_seq(self, seq, size):
        start = 0
        end = start + size
        newseq = []
        while (seq[start:end] != []):
            newseq.append(seq[start:end])
            start = end+1
            end = end+size
        return newseq

    def bulk_get_tokens(self, tokens):
        new_tokens = []
        for token_pair in tokens:
            token1 = xcode(token_pair[0]) if isinstance(token_pair[0], unicode) else token_pair[0]
            token1 = token1.lower()

            token2 = xcode(token_pair[1]) if isinstance(token_pair[1], unicode) else token_pair[1]
            token2 = token2.lower()
            new_tokens.append([token1, token2])
        return new_tokens

class HealthCheck(RequestHandler):
    def initialize(self, args):
        self.args = args

    @gen.coroutine
    def check_status(self):
        http_client = AsyncHTTPClient()

        fn, query = self.args.fn_name_query.split(",")
        _url = HEALTH_CHECK_URL % (self.args.port, fn, query)

        response = yield http_client.fetch(_url)
        if response.code == SUCCESS_CODE:
            self.set_status(SUCCESS_CODE)
        else:
            self.set_status(ERROR_CODE)

        return

    get = post = check_status

class CooccurrenceServer(Server):

    DESC = 'For an input token,this service provides the number of documents that occurred(Document Occurrences) at least once in the given corpus.For input of 2 tokens,it provides the documents count where these 2 tokens cooccurred(Document Co-occurrences) at least once in the given corpus.'

    def define_args(self, parser):
        super(CooccurrenceServer, self).define_args(parser)

        parser.add_argument('--dir-path',
                    help='Path to directory with all data files')
        parser.add_argument('--fn-name-query',
                    help='Provide function and inputs params '
                        'with comma seperated to get the '
                        'healthcheck status of the service')

    def prepare_handlers(self):
         return [
            (r"/healthcheck", HealthCheck, dict(args=self.args)),
        ]

    def prepare_api(self):
        super(CooccurrenceServer, self).prepare_api()

        return CooccurrenceServerAPI(
            self.args.dir_path, self.log, self.args)

if __name__ == '__main__':
    CooccurrenceServer().start()
