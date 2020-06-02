# -*- coding: utf-8 -*-
"""
Created on Sat Apr  7 20:25:17 2018

@author: Stanislav
"""
import re
import numpy as np
import math
import pickle
import os

from typing import Tuple, Dict, List, Any

from tensorflow.keras.models import load_model  # type: ignore
from tensorflow.keras import backend as K  # type: ignore
from tensorflow.keras.preprocessing import sequence  # type: ignore
from tensorflow.keras.models import Model  # type: ignore

ParentTag = str
ChildTag = str


class income_st_builder():
    rules: Dict[str, str] = {}
    tag_to_code: Dict[str, int] = {}
    known_errors: List[Tuple[str, str, str]] = []
    potential_parents: List[Tuple[str]] = []
    model_statement_classification: Any = None
    model_sign_classification: Any = None
    is_ready: bool = True
    error_message: str = ''

    def __init__(self, data_path: str):
        result, self.error_message = self.load_models(data_path)
        if not result:
            self.is_ready = False

    def load_models(self, data_path: str) -> Tuple[bool, str]:
        models_load_succesful = True
        data_load_successful = True
        data_array = []
        error_message = ''

        try:
            with open(os.path.join(data_path, 'income_st_data.txt'), 'rb') as fp:
                data_array = pickle.load(fp)
                fp.close()
        except Exception as e:
            error_message = error_message + ' ' + str(e)
            data_load_successful == False

        if not data_load_successful or len(data_array) < 4:
            return False, error_message

        self.rules = data_array[0]
        self.tag_to_code = data_array[1]
        self.known_errors = data_array[2]
        self.potential_parents = data_array[3]

        try:
            self.model_statement_classification = load_model(
                os.path.join(data_path, 'income_st_model0.h5'))
            #self.models_dictionary.update({0: model_statement_classification})
            self.model_sign_classification = load_model(
                os.path.join(data_path, 'income_st_model1.h5'))
            #self.models_dictionary.update({1: model_sign_classification})
        except Exception as e:
            error_message = str(e)
            models_load_succesful = False

        if not models_load_succesful:
            return False, error_message

        return True, error_message

    def al_build_tree(
            self, all_tags) -> Tuple[List[Tuple[ChildTag, ParentTag]], List[str]]:
        trees_out, nodes_out = self.build_tree(
            all_tags, self.model_statement_classification, self.tag_to_code,
            self.rules, 18, 64)
        return(trees_out, nodes_out)

    def al_add_weights(self, trees):
        trees_out = self.append_weights(
            trees,
            self.model_sign_classification,
            self.tag_to_code,
            self.rules,
            15,
            64)
        return(trees_out)

    # dependencies reconstruct, get_best_parent_v2, extract_tree, prepare_data
    # returns list of trees (child, parent), list of lost nodes
    def build_tree(
            self,
            all_tags,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        if 'NetIncomeLoss' not in all_tags:
            all_tags.append('NetIncomeLoss')

        reconstructed = self.reconstruct(
            all_tags,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size)
        pairs_constructed = []
        nodes_left = []  # элементы, у которых нет родителей
        trees_to_analyse = []  # получившиеся поддеревья
        nodes_out = []  # одиночные элементы
        trees_out = []  # выходные деревья
        tree_with_NIL_index = -1  # индекс дерева, в котором находится NetIncomeLoss

        all_childs = list(set([x[0] for x in reconstructed]))
        for child in all_childs:
            all_parents = list(
                set([x[1] for x in reconstructed if x[1] != '' and x[0] == child and x[2] == 1]))
            if len(all_parents) > 1:
                # у листа 2 и более родителя
                best_parent_det, selected_parents = self.get_best_parent_v2(
                    child, all_parents, class_model, tag_to_code, rules, max_len, batch_size)
                pairs_constructed.append((child, best_parent_det))
            elif len(all_parents) == 1:
                pairs_constructed.append((child, all_parents[0]))
            elif len(all_parents) == 0:
                nodes_left.append(child)

        # смтрим каждый элемент без родителя и разбираемся что это
        for node in nodes_left:
            tree = self.extract_tree(node, pairs_constructed)
            if len(tree) > 0:
                trees_to_analyse.append((node, tree))
                search_for_NIL = [x for x in tree if x[0] ==
                                  'NetIncomeLoss' or x[1] == 'NetIncomeLoss']
                if len(search_for_NIL) > 0:
                    tree_with_NIL_index = len(trees_to_analyse) - 1
            else:
                nodes_out.append(node)
        # теперь имеем список деревьев, номер дерева, в котором оказался
        # NetIncomeLoss
        if tree_with_NIL_index < 0:
            # нет деревьев, привязанных к NetIncomeLoss
            for tree in trees_to_analyse:
                trees_out.append(tree[1])
        else:
            # берем дерево с NetIncomeLoss и пытаемся к нему подсоединить
            # остальные
            trees_out.append(trees_to_analyse[tree_with_NIL_index][1])
            united_tree = trees_to_analyse[tree_with_NIL_index][1]
            for i in range(len(trees_to_analyse)):
                if i != tree_with_NIL_index:
                    NIL_tags = self.prepare_data(
                        trees_to_analyse[tree_with_NIL_index][1])
                    NIL_tags.append(trees_to_analyse[i][0])
                    reconstructed_subs = self.reconstruct(
                        NIL_tags, class_model, tag_to_code, rules, max_len, batch_size)
                    sub_tree_parents = list(
                        set(
                            [x[1]
                             for x in reconstructed_subs
                             if x[1] != '' and x[0] ==
                             trees_to_analyse[i][0] and
                             x[2] == 1]))
                    # print(sub_tree_parents)
                    if len(sub_tree_parents) == 0:
                        trees_out.append(trees_to_analyse[i][1])
                    else:
                        united_tree = united_tree + trees_to_analyse[i][1]
                        united_tree.append(
                            (trees_to_analyse[i][0], sub_tree_parents[0]))
            trees_out[0] = united_tree
        return(trees_out, nodes_out)

    # dependencies get_predictions_weight
    # returns list of trees with format child,parent,weight
    def append_weights(
            self,
            trees,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        trees_out = []
        for tree in trees:
            predicted = self.get_predictions_weight(
                tree, class_model, tag_to_code, rules, max_len, batch_size)
            predicted_int = []
            for i in range(predicted.shape[0]):
                if predicted[i] >= 0.5:
                    predicted_int.append(1)
                else:
                    predicted_int.append(-1)
            trees_out.append(
                list(zip([x[0] for x in tree], [x[1] for x in tree], predicted_int)))
        return(trees_out)

    # dependencies parse_tag_without_y, sequence.pad_sequences
    # returns array of predictions
    def get_predictions_weight(
            self,
            tree,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        tags_childs = []
        tags_parents = []
        for pair in tree:
            tags_childs.append(pair[0])
            tags_parents.append(pair[1])
        x_parents = self.parse_tag_without_y(tags_parents, tag_to_code, rules)
        x_childs = self.parse_tag_without_y(tags_childs, tag_to_code, rules)
        x_parents = sequence.pad_sequences(x_parents, maxlen=max_len)
        x_childs = sequence.pad_sequences(x_childs, maxlen=max_len)

        # добивка до batch_size
        full_batches = math.trunc(len(tags_parents) / batch_size)

        x_parents_fake = np.zeros(((full_batches + 1) * batch_size, max_len))
        x_childs_fake = np.zeros(((full_batches + 1) * batch_size, max_len))

        x_parents_fake[0:x_parents.shape[0], :] = x_parents
        x_childs_fake[0:x_childs.shape[0], :] = x_childs
        predicted = class_model.predict(
            [x_childs_fake, x_parents_fake],
            batch_size=batch_size)
        return(predicted[0:len(tags_parents)])

    # dependencies reconstruct
    # returns best_parent (string), potential parents determined by network
    # (list of strings)

    def get_best_parent_v2(
            self,
            child,
            potential_parents,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        all_tags = []
        all_tags.append(child)
        all_tags = all_tags + potential_parents
        # child,parent,class,states[context,result]
        reconstructed = self.reconstruct(
            all_tags,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size)
        selected_parents = [(x[1], x[3])
                            for x in reconstructed if x[0] == child and x[2] == 1]
        max_index = -1
        max_probablity = 0
        best_parent = ''
        for i in range(len(selected_parents)):
            sum_probability = 0
            for prob in selected_parents[i][1]:
                sum_probability = sum_probability + prob[1]
            if sum_probability > max_probablity:
                max_index = i
                max_probablity = sum_probability
        if max_index == -1:
            best_parent = ''
        else:
            best_parent = selected_parents[max_index][0]
        return(best_parent, selected_parents)

    # dependencies get_combinations,get_predictions
    # returns list with structure child,parent,class,states[context,result]
    def reconstruct(
            self,
            all_tags,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        reconstructed = []
        combs = self.get_combinations(len(all_tags))
        triples = []
        for elem in combs:
            triples.append(
                (all_tags[elem[0]],
                 all_tags[elem[1]],
                 all_tags[elem[2]]))
        tags_parents = [x[0] for x in triples]
        tags_childs = [x[1] for x in triples]
        tags_contexts = [x[2] for x in triples]
        predicted = self.get_predictions(
            tags_parents,
            tags_childs,
            tags_contexts,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size)
        zipped = list(zip(tags_childs, tags_parents, tags_contexts, predicted))
        for elem in all_tags:
            cur_parents = list(set([x[1] for x in zipped if x[0] == elem]))
            cur_sets = [(x[1], x[2], x[3]) for x in zipped if x[0] == elem]
            for parent in cur_parents:
                cur_states = [(x[1], x[2][0])
                              for x in cur_sets if x[0] == parent]
                states_less05 = [x for x in cur_states if x[1] < 0.4]
                if len(states_less05) == 0:
                    reconstructed.append((elem, parent, 1, cur_states))
                else:
                    reconstructed.append((elem, parent, 0, cur_states))
        return(reconstructed)

    # returns list with all combinations of tags
    def get_combinations(self, tags_number):
        combs = []
        for i in range(tags_number):
            for j in range(tags_number):
                for k in range(tags_number):
                    if i != j and j != k and i != k:
                        combs.append((i, j, k))
        return(combs)

    # dependencies parse_tag_without_y, sequence.pad_sequences, math, numpy
    # returns numpy array of float
    def get_predictions(
            self,
            tags_parents,
            tags_childs,
            tags_contexts,
            class_model,
            tag_to_code,
            rules,
            max_len,
            batch_size):
        x_parents = self.parse_tag_without_y(tags_parents, tag_to_code, rules)
        x_childs = self.parse_tag_without_y(tags_childs, tag_to_code, rules)
        x_contexts = self.parse_tag_without_y(
            tags_contexts, tag_to_code, rules)
        x_parents = sequence.pad_sequences(x_parents, maxlen=max_len)
        x_childs = sequence.pad_sequences(x_childs, maxlen=max_len)
        x_contexts = sequence.pad_sequences(x_contexts, maxlen=max_len)

        # добивка до batch_size
        full_batches = math.trunc(len(tags_parents) / batch_size)

        x_parents_fake = np.zeros(((full_batches + 1) * batch_size, max_len))
        x_childs_fake = np.zeros(((full_batches + 1) * batch_size, max_len))
        x_contexts_fake = np.zeros(((full_batches + 1) * batch_size, max_len))

        x_parents_fake[0:x_parents.shape[0], :] = x_parents
        x_childs_fake[0:x_childs.shape[0], :] = x_childs
        x_contexts_fake[0:x_childs.shape[0], :] = x_contexts
        predicted = class_model.predict(
            [x_parents_fake, x_childs_fake, x_contexts_fake],
            batch_size=batch_size)
        return(predicted[0:len(tags_parents)])

    # dependencies re
    # list of lists with tags coded
    def parse_tag_without_y(self, tags_list, tag_to_code, rules):
        all_x = []

        for elem in tags_list:
            t = re.findall('[A-Z][^A-Z]*', elem)
            t1 = [rules.get(j, j) for j in t]
            t2 = ''.join(t1)
            t3 = re.findall('[A-Z][^A-Z]*', t2)
            k = [tag_to_code.get(j, 1) for j in t3]
            all_x.append(k)
        return(all_x)

    def extract_tree(self, node, pairs):
        first_chains = [x for x in pairs if x[1] == node]
        if len(first_chains) == 0:
            return([])
        tree_extracted = []
        self.extract_recourse(node, pairs, tree_extracted)
        return(tree_extracted)

    def extract_recourse(self, node, pairs, tree_extracted):
        next_chains = [x for x in pairs if x[1] == node]
        if len(next_chains) > 0:
            for elem in next_chains:
                tree_extracted.append(elem)
            #tree_extracted = tree_extracted+next_chains
            for elem in next_chains:
                self.extract_recourse(elem[0], pairs, tree_extracted)

    def prepare_data(self, tree):
        parents = [x[0] for x in tree]
        childs = [x[1] for x in tree]
        all_tags = list(set(parents + childs))
        return(all_tags)
