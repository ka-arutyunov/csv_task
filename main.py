# !/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import os
from http.server import SimpleHTTPRequestHandler
from itertools import islice
from socketserver import TCPServer
from urllib.parse import urlparse, parse_qs


class DataClass(object):
    """
    Create Singletone object

    """
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DataClass, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.index_file = 'index.csv'
        self.sorted_file = 'result.csv'
        self.input_file = 'recommends.csv'
        self.lines_in_little_file = 6 * 10 ** 6

        self.out_files = self.split_to_sorted_files(self.input_file,
                                                    self.lines_in_little_file)
        self.merge_files(self.sorted_file, self.out_files)
        self.create_index(self.sorted_file, self.index_file)

        self.all_sku_dct = {}
        self.index_load(self.index_file, self.all_sku_dct)

    @staticmethod
    def sort_condition(x) -> str:
        """
        This function is condition for sort data, now its sort by sku.
        x should not be empty.

        """
        return x[0]

    def split_to_sorted_files(self, filename, read_buffer) -> list:
        """
        This function split big unsorted text file to many sorted files.

        """
        file_list = []
        with open(filename, 'r', encoding='utf8') as in_file:
            file_number = 0
            while True:
                s = islice(in_file, 0, read_buffer)
                data = list(map(lambda x: x.split(','), list(s)))
                if not data:
                    break
                out_file_name = f'{file_number}_tmp.csv'
                file_list.append(out_file_name)
                with open(out_file_name, 'w',
                          encoding='utf8',
                          newline='\n') as f:
                    f.writelines([','.join(item)
                                  for item in sorted(data,
                                                     key=self.sort_condition)])
                file_number += 1
        return file_list

    def merge_files(self, filename, files) -> int:
        """
        This function merge sorted many files in a list to big sorted file.

        """
        with open(filename, 'w', encoding='utf8', newline='\n') as out:
            opened = [open(file, 'r', encoding='utf8') for file in files]
            lines_to_print = [
                file.readline().split(',') + [str(k)]
                for k, file in enumerate(opened)
            ]
            while True:
                if not lines_to_print:
                    break
                min_line = min(lines_to_print, key=self.sort_condition)
                lines_to_print.remove(min_line)
                out.write(','.join(min_line[:3]))
                tmp = opened[int(min_line[3])].readline()
                if not tmp:
                    continue
                lines_to_print += [tmp.split(',') + [min_line[3]]]
            for file in opened:
                file.close()
        for each in files:
            if os.path.isfile(each):
                os.remove(each)
        return 0

    def create_index(self, sorted_file_name, index_file_name) -> int:
        """
        Create index file for faster search

        """
        with open(sorted_file_name, 'r', encoding='utf8') as in_file:
            start = end = 0
            result = []
            sku = ''
            s = islice(in_file, 0, None)
            for i, line in enumerate(s):
                tmp_sku = line.split(',')[0]
                if tmp_sku != sku and i != 0:
                    result.append([sku, str(start), str(end) + '\n'])
                    start = i
                end, sku = i, tmp_sku
            result.append([sku, str(start), str(end) + '\n'])
            with open(index_file_name, 'w',
                      encoding='utf8',
                      newline='\n') as out:
                out.writelines([','.join(item) for item in result])
        return 0

    def index_load(self, index_filename, dct) -> int:
        """
        Loads index into dictionary

        """
        with open(index_filename, 'r', encoding='utf8') as in_file:
            s = islice(in_file, 0, None)
            for line in s:
                tmp = line.rstrip().split(',')
                dct[tmp[0]] = {'start': int(tmp[1]), 'end': int(tmp[2])}
        return 0

    def search_in_file(self,
                       sorted_filename,
                       index_dct,
                       sku,
                       rank=0.0) -> list:
        """This function will check sku in dict and return slice with
             values from sorted file with rank higher than in params.

        """
        sku_recommends = []
        if sku in index_dct:
            sku_result = index_dct[sku]
            with open(sorted_filename, 'r', encoding='utf8') as in_file:
                str_length = 26
                in_file.seek(str_length * sku_result['start'])
                for item in in_file:
                    tmp = item.strip().split(',')
                    if tmp[0] != sku:
                        break
                    sku_recommends.append((tmp[1], float(tmp[2])))

        return sorted([i for i in sku_recommends if i[1] > rank],
                      key=lambda x: x[1])


class HttpRequestHandler(SimpleHTTPRequestHandler):
    def response(self, status) -> None:
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        query_sku = query.get('sku')[0] if 'sku' in query else ''
        query_rank = query.get('rank')[0] if 'rank' in query else 0.0
        try:
            query_rank = float(query_rank)
        except ValueError:
            query_rank = 0.0
        recommends = instance.search_in_file(instance.sorted_file,
                                             instance.all_sku_dct,
                                             query_sku,
                                             query_rank)
        self.response(400) if not recommends else self.response(200)
        self.wfile.write(json.dumps(recommends).encode('utf-8'))


def run_server() -> None:
    object_handler = HttpRequestHandler
    port = 8080
    server = TCPServer(("", port), object_handler)
    print(f'Server running on http://localhost:{port}')
    server.serve_forever()


if __name__ == '__main__':
    instance = DataClass()
    run_server()
