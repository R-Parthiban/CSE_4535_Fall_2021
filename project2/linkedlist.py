'''
@author: Sougata Saha
Institute: University at Buffalo
'''

import math


class Node:

    def __init__(self, value=None, next=None, doc_len=None,tf_idf=0.0):
        """ Class to define the structure of each node in a linked list (postings list).
            Value: document id, Next: Pointer to the next node
            Add more parameters if needed.
            Hint: You may want to define skip pointers & appropriate score calculation here"""
        self.value = value
        self.next = next
        self.skip = None
        self.tf = 0.0
        self.term_count_in_doc = 1
        self.doc_len = doc_len
        self.tf_idf = tf_idf


class LinkedList:
    """ Class to define a linked list (postings list). Each element in the linked list is of the type 'Node'
        Each term in the inverted index has an associated linked list object.
        Feel free to add additional functions to this class."""
    def __init__(self):
        self.start_node = None
        self.end_node = None
        self.length, self.n_skips, self.idf = 0, 0, 0.0
        self.skip_length = None

    def traverse_list(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            """ Write logic to traverse the linked list.
                To be implemented."""
            n = self.start_node
            while n is not None:
                traversal.append(n.value)
                n = n.next
            return traversal

    def traverse_skips(self):
        traversal = []
        if self.start_node is None:
            return
        else:
            node = self.start_node
            traversal.append(node.value)
            while node is not None:
                if node.skip is not None:
                    node = node.skip
                    traversal.append(node.value)
                else:
                    node = node.next
        if len(traversal) is 1:
            return []
        return traversal

    def add_skip_connections(self):
        self.n_skips = math.floor(math.sqrt(self.length))
        if self.n_skips * self.n_skips == self.length:
            self.n_skips = self.n_skips - 1
        skip_length = math.floor(self.length / math.ceil(math.sqrt(self.length)))
        if skip_length * skip_length == self.length:
            skip_length = skip_length - 1

        """ Write logic to add skip pointers to the linked list.
            This function does not return anything.
            To be implemented."""
        if skip_length < 1:
            return
        else:
            skip_length += 1
            index = 1
            current_node, traverse_node = self.start_node, self.start_node.next
            while traverse_node is not None:
                if index % skip_length == 0:
                    current_node.skip = traverse_node
                    current_node = traverse_node
                traverse_node = traverse_node.next
                index += 1

    def insert_data(self,value):
        new_node = Node(value=value)
        if self.start_node is None:
            self.start_node = new_node
            return
        end = self.start_node
        while end.next:
            end = end.next
        end.next = new_node

    def insert_at_end(self, value, doc_length,initial_tfidf):
        """ Write logic to add new elements to the linked list.
            Insert the element at an appropriate position, such that elements to the left are lower than the inserted
            element, and elements to the right are greater than the inserted element.
            To be implemented. """
        # node_exist = self.get_node(value)
        # if not node_exist == None:
        #     return
        #     #node_exist.
        check_node_exist = self.check_node(value)
        if check_node_exist is not None:
            check_node_exist.term_count_in_doc += 1
            return
        new_node = Node(value=value, doc_len=doc_length, tf_idf=initial_tfidf)
        n = self.start_node
        self.length += 1

        if self.start_node is None:
            self.start_node = new_node
            self.end_node = new_node
            return

        elif self.start_node.value >= value:
            self.start_node = new_node
            self.start_node.next = n
            return

        elif self.end_node.value <= value:
            self.end_node.next = new_node
            self.end_node = new_node
            return

        else:
            while n.value < value < self.end_node.value and n.next is not None:
                n = n.next

            m = self.start_node
            while m.next != n and m.next is not None:
                m = m.next
            m.next = new_node
            new_node.next = n
            return

    def check_node(self, value):
        head = self.start_node
        while head is not None:
            if head.value == value:
                return head
            head = head.next
        return None

    def calculate_tf_tf_idf(self):
        head = self.start_node
        while head is not None:
            head.tf = head.term_count_in_doc / head.doc_len
            head.tf_idf = head.tf * self.idf
            head = head.next
