'''
@author: Sougata Saha
Institute: University at Buffalo
'''

from tqdm import tqdm
from preprocessor import Preprocessor
from indexer import Indexer
from collections import OrderedDict
from linkedlist import LinkedList
import inspect as inspector
import sys
import argparse
import json
import time
import random
import flask
from flask import Flask
from flask import request
import hashlib

app = Flask(__name__)




class ProjectRunner:
    def __init__(self):
        self.preprocessor = Preprocessor()
        self.indexer = Indexer()

    def _merge(self,list1,list2, withSkip=False):
        """ Implement the merge algorithm to merge 2 postings list at a time.
            Use appropriate parameters & return types.
            While merging 2 postings list, preserve the maximum tf-idf value of a document.
            To be implemented."""
        intersection_list = LinkedList()
        comp_count = 0
        if withSkip:
            if list1 is not None and list2 is not None:
                p1 = list1.start_node
                p2 = list2.start_node
                while p1 and p2:
                    if p1.value == p2.value:
                        if p1.tf_idf > p2.tf_idf:
                            intersection_list.insert_at_end(p1.value, 0, p1.tf_idf)
                        else:
                            intersection_list.insert_at_end(p1.value, 0, p2.tf_idf)
                        p1 = p1.next
                        p2 = p2.next
                    elif p1.value < p2.value:
                        if ( p1.skip is not None) and (p1.skip.value <= p2.value):
                            while p1.skip.value <= p2.value:
                                p1 = p1.skip
                                if not p1.value == p2.value:
                                    comp_count += 1
                                if p1.skip is None:
                                    break
                            if not p1.value == p2.value:
                                comp_count -= 1
                        else:
                            p1 = p1.next
                    else:
                        if (p2.skip is not None) and (p2.skip.value <= p1.value):
                            while p2.skip.value <= p1.value:
                                p2 = p2.skip
                                if not p1.value == p2.value:
                                    comp_count += 1
                                if p2.skip is None:
                                    break
                            if not p1.value == p2.value:
                                comp_count -= 1
                        else:
                            p2 = p2.next
                    comp_count += 1
            return intersection_list, comp_count
        else:
            if list1 is not None and list2 is not None:
                p1 = list1.start_node
                p2 = list2.start_node
                while p1 and p2:
                    if p1.value == p2.value:
                        intersection_list.insert_at_end(p1.value, 0,p1.tf_idf)
                        p1 = p1.next
                        p2 = p2.next
                    elif p1.value < p2.value:
                        p1 = p1.next
                    else:
                        p2 = p2.next
                    comp_count += 1
            return intersection_list, comp_count

    def _daat_and(self, query_list, withSkip=False, tf_idf_sorting=False):
        """ Implement the DAAT AND algorithm, which merges the postings list of N query terms.
            Use appropriate parameters & return types.
            To be implemented."""
        #Get length posting list for each term
        merged_linked_list, final_merge_result = None, None
        new_comparison, total_comparison = 0, 0
        _map_length_term = {}
        _posting_list_length = []
        for term in query_list:
            posting_linkedList = self.indexer.inverted_index.get(term)
            _map_length_term[posting_linkedList.length] = posting_linkedList
            _posting_list_length.append(posting_linkedList.length)
        _posting_list_length = sorted(_posting_list_length)
        for i in range(len(_posting_list_length)):
            if i == 0:
                merged_linked_list, new_comparison = self._merge(_map_length_term.get(_posting_list_length[i]),
                                                                 _map_length_term.get(_posting_list_length[i + 1]), withSkip)
                total_comparison = total_comparison + new_comparison
                final_merge_result = merged_linked_list
                i += 1
            if i > 1:
                final_merge_result, new_comparison = self._merge(final_merge_result,
                                                                     _map_length_term.get(_posting_list_length[i]),
                                                                     withSkip)
                total_comparison = total_comparison + new_comparison
            # if i == 2:
            #     final_merge_result, new_comparison = self._merge(merged_linked_list,
            #                                                      _map_length_term.get(_posting_list_length[i]), withSkip)
            #     total_comparison = total_comparison + new_comparison
            # if i > 2:
            #     final_merge_result, new_comparison = self._merge(final_merge_result,
            #                                                      _map_length_term.get(_posting_list_length[i]),
            #                                                      withSkip)
            #     total_comparison = total_comparison + new_comparison
        if tf_idf_sorting:
            sorted_merged_list = self.sort_by_tfidf_weight(final_merge_result)
            return sorted_merged_list, total_comparison
        return final_merge_result.traverse_list(), total_comparison

    def sort_by_tfidf_weight(self,final_merge_result):
        list_nodes = []
        sorted_by_tfidf = []
        head = final_merge_result.start_node
        while head is not None:
            list_nodes.append(head)
            head = head.next
        for i in range(0, len(list_nodes)):
            max_index = i
            for j in range(i + 1, len(list_nodes)):
                if list_nodes[max_index].tf_idf == list_nodes[j].tf_idf:
                    if list_nodes[max_index].value > list_nodes[j].value:
                        max_index = j
                if list_nodes[max_index].tf_idf < list_nodes[j].tf_idf:
                    max_index = j
            list_nodes[i], list_nodes[max_index] = list_nodes[max_index], list_nodes[i]
        for node in list_nodes:
            sorted_by_tfidf.append(node.value)
        return sorted_by_tfidf


    def _get_postings(self, queryTerm):
        """ Function to get the postings list of a term from the index.
            Use appropriate parameters & return types.
            To be implemented."""
        if queryTerm in self.indexer.inverted_index.keys():
            query_posting_list = self.indexer.inverted_index.get(queryTerm)
            return query_posting_list.traverse_list()
        return None
        # raise NotImplementedError

    def _get_postings_with_skip(self, term):
        if term in self.indexer.inverted_index.keys():
            query_posting_list = self.indexer.inverted_index.get(term)
            return query_posting_list.traverse_skips()
        return None

    def _output_formatter(self, op):
        """ This formats the result in the required format.
            Do NOT change."""
        if op is None or len(op) == 0:
            return [], 0
        op_no_score = [int(i) for i in op]
        results_cnt = len(op_no_score)
        return op_no_score, results_cnt

    def run_indexer(self, corpus):
        """ This function reads & indexes the corpus. After creating the inverted index,
            it sorts the index by the terms, add skip pointers, and calculates the tf-idf scores.
            Already implemented, but you can modify the orchestration, as you seem fit."""
        doc_length = 0
        with open(corpus, 'r') as fp:
            for line in tqdm(fp.readlines()):
                doc_length += 1
                doc_id, document = self.preprocessor.get_doc_id(line)
                tokenized_document = self.preprocessor.tokenizer(document)
                self.indexer.generate_inverted_index(doc_id, tokenized_document)
        self.indexer.sort_terms()
        self.indexer.add_skip_connections()
        self.indexer.calculate_tf_idf(doc_length)

    def sanity_checker(self, command):
        """ DO NOT MODIFY THIS. THIS IS USED BY THE GRADER. """

        index = self.indexer.get_index()
        kw = random.choice(list(index.keys()))
        return {"index_type": str(type(index)),
                "indexer_type": str(type(self.indexer)),
                "post_mem": str(index[kw]),
                "post_type": str(type(index[kw])),
                "node_mem": str(index[kw].start_node),
                "node_type": str(type(index[kw].start_node)),
                "node_value": str(index[kw].start_node.value),
                "command_result": eval(command) if "." in command else ""}
               # "command_result": "2"}

    def run_queries(self, query_list, random_command):
        """ DO NOT CHANGE THE output_dict definition"""
        output_dict = {'postingsList': {},
                       'postingsListSkip': {},
                       'daatAnd': {},
                       'daatAndSkip': {},
                       'daatAndTfIdf': {},
                       'daatAndSkipTfIdf': {},
                       'sanity': self.sanity_checker(random_command)}

        for query in tqdm(query_list):
            """ Run each query against the index. You should do the following for each query:
                1. Pre-process & tokenize the query.
                2. For each query token, get the postings list & postings list with skip pointers.
                3. Get the DAAT AND query results & number of comparisons with & without skip pointers.
                4. Get the DAAT AND query results & number of comparisons with & without skip pointers, 
                    along with sorting by tf-idf scores."""
            # raise NotImplementedError

            input_term_arr = self.preprocessor.tokenizer(query)  # Tokenized query. To be implemented.

            for term in input_term_arr:
                postings, skip_postings = None, None

                """ Implement logic to populate initialize the above variables.
                    The below code formats your result to the required format.
                    To be implemented."""
                postings = self._get_postings(term)
                skip_postings = self._get_postings_with_skip(term)
                output_dict['postingsList'][term] = postings
                output_dict['postingsListSkip'][term] = skip_postings

            and_op_no_skip, and_op_skip, and_op_no_skip_sorted, and_op_skip_sorted = None, None, None, None
            and_comparisons_no_skip, and_comparisons_skip, \
            and_comparisons_no_skip_sorted, and_comparisons_skip_sorted = None, None, None, None
            """ Implement logic to populate initialize the above variables.
                The below code formats your result to the required format.
                To be implemented."""

            and_op_no_skip, and_comparisons_no_skip = self._daat_and(input_term_arr, False,False)
            and_op_skip, and_comparisons_skip = self._daat_and(input_term_arr, True,False)
            and_op_no_skip_sorted, and_comparisons_no_skip_sorted = self._daat_and(input_term_arr, False,True)
            and_op_skip_sorted, and_comparisons_skip_sorted = self._daat_and(input_term_arr, True, True)

            and_op_no_score_no_skip, and_results_cnt_no_skip = self._output_formatter(and_op_no_skip)
            and_op_no_score_skip, and_results_cnt_skip = self._output_formatter(and_op_skip)
            and_op_no_score_no_skip_sorted, and_results_cnt_no_skip_sorted = self._output_formatter(
                and_op_no_skip_sorted)
            and_op_no_score_skip_sorted, and_results_cnt_skip_sorted = self._output_formatter(and_op_skip_sorted)

            output_dict['daatAnd'][query.strip()] = {}
            output_dict['daatAnd'][query.strip()]['results'] = and_op_no_score_no_skip
            output_dict['daatAnd'][query.strip()]['num_docs'] = and_results_cnt_no_skip
            output_dict['daatAnd'][query.strip()]['num_comparisons'] = and_comparisons_no_skip

            output_dict['daatAndSkip'][query.strip()] = {}
            output_dict['daatAndSkip'][query.strip()]['results'] = and_op_no_score_skip
            output_dict['daatAndSkip'][query.strip()]['num_docs'] = and_results_cnt_skip
            output_dict['daatAndSkip'][query.strip()]['num_comparisons'] = and_comparisons_skip

            output_dict['daatAndTfIdf'][query.strip()] = {}
            output_dict['daatAndTfIdf'][query.strip()]['results'] = and_op_no_score_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_docs'] = and_results_cnt_no_skip_sorted
            output_dict['daatAndTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_no_skip_sorted

            output_dict['daatAndSkipTfIdf'][query.strip()] = {}
            output_dict['daatAndSkipTfIdf'][query.strip()]['results'] = and_op_no_score_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_docs'] = and_results_cnt_skip_sorted
            output_dict['daatAndSkipTfIdf'][query.strip()]['num_comparisons'] = and_comparisons_skip_sorted
        return output_dict


@app.route("/execute_query", methods=['POST'])
def execute_query():
    """ This function handles the POST request to your endpoint.
        Do NOT change it."""
    start_time = time.time()

    queries = request.json["queries"]
    random_command = request.json["random_command"]

    """ Running the queries against the pre-loaded index. """
    output_dict = runner.run_queries(queries, random_command)

    """ Dumping the results to a JSON file. """
    with open(output_location, 'w') as fp:
        json.dump(output_dict, fp)

    response = {
        "Response": output_dict,
        "time_taken": str(time.time() - start_time),
        "username_hash": username_hash
    }
    return flask.jsonify(response)


if __name__ == "__main__":
    """ Driver code for the project, which defines the global variables.
        Do NOT change it."""

    output_location = "project2_output.json"
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--corpus", type=str, help="Corpus File name, with path.")
    parser.add_argument("--output_location", type=str, help="Output file name.", default=output_location)
    parser.add_argument("--username", type=str,
                        help="Your UB username. It's the part of your UB email id before the @buffalo.edu. "
                             "DO NOT pass incorrect value here")

    argv = parser.parse_args()

    corpus = argv.corpus
    output_location = argv.output_location
    username_hash = hashlib.md5(argv.username.encode()).hexdigest()

    """ Initialize the project runner"""
    runner = ProjectRunner()

    """ Index the documents from beforehand. When the API endpoint is hit, queries are run against 
        this pre-loaded in memory index. """
    runner.run_indexer(corpus)
    # output_dict = runner.run_queries(["sars-cov-2 protein structure"],0)
    # #output_dict = runner.run_queries([" the novel coronavirus ", "from an epidemic to a pandemic", "is hydroxychloroquine effective?"], 0)
    # print("2")
    # with open(output_location, 'w') as fp:
    #     json.dump(output_dict, fp)

    app.run(host="0.0.0.0", port=9999)
