# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re
from datetime import datetime
import pickle

from typing import List, Any, Dict

from tensorflow.keras.models import load_model, Sequential  # type: ignore
# from tensorflow.keras.layers import Dense, Embedding, TimeDistributed
# from tensorflow.keras.layers import LSTM
from tensorflow.keras.utils import to_categorical  # type: ignore
from random import shuffle
from tensorflow.keras import backend as K  # type: ignore


class Option_parser:
    error_messages: List[str] = []
    state = 0
    check_words: List[Any] = []
    word_to_code: Dict[str, int] = {}
    max_features = 0
    max_len = 0
    all_words_stats: List[str] = []
    model = Sequential()
    rules: Dict[str, str] = {}
    parsed_tables: List[Any] = []

    def __init__(self, model_path, dict_path):
        result, messages, check_words, word_to_code, max_features, max_len, all_words_stats, model, rules = self.load_files(
            model_path, dict_path)
        if not result:
            self.error_messages.append(messages)
            self.state = 1
        else:
            self.check_words = check_words
            self.word_to_code = word_to_code
            self.max_features = max_features
            self.max_len = max_len
            self.model = model
            self.all_words_stats = all_words_stats
            self.rules = rules

    def clear_history(self):
        self.error_messages = []
        self.state = 0

    def get_option_exercise_price(self, table_num):
        str_found = ''
        if table_num < 0 or table_num > len(self.parsed_tables) - 1:
            self.error_messages.append('No such table')
            return('', 0, False)
        else:
            row_num, column_num, confidence_row, confidence_column, best_year_num, best_year_confidence = self.get_execution_price(
                self.parsed_tables[table_num][1], self.parsed_tables[table_num][2], self.parsed_tables[table_num][3], self.word_to_code, self.max_len, self.rules, self.model)
            confidence = confidence_row and confidence_column
            if row_num > -1 and column_num > -1 and confidence:
                str_found = (self.parsed_tables[table_num][1])[
                    column_num][row_num]
            return(str_found, best_year_num, best_year_confidence)
        # возвращаемые значения str_found, confidence, best_year,
        # year_confidence

    def parse_html(self, html_string):
        self.parsed_tables = []
        df_list = []
        table_found = True
        try:
            df_list = pd.read_html(html_string)
        except Exception as e:
            table_found = False
            self.error_messages.append(str(e))
        if not table_found:
            self.state = 1
        elif len(df_list) > 0:
            for df_num in range(len(df_list)):
                df_toclear = df_list[df_num].copy()
                df_toclear = self.delete_empty_column(df_toclear)
                df_toclear = self.delete_empty_row(df_toclear)
                if len(df_toclear.columns) < 2 or len(df_toclear.index) < 2:
                    self.error_messages.append(
                        'Table ' + str(df_num) +
                        ' have less than 2 columns or rows')
                    self.state = 2
                else:
                    lead_index = df_toclear.columns[0]
                    new_header, head_rows = self.modify_head(
                        df_toclear, self.check_words, lead_index)
                    if len(head_rows) == 0:
                        self.error_messages.append(
                            'Head rows were not identified')
                        self.state = 3
                    else:
                        df_toclear, new_header = self.clear_table(
                            df_toclear, lead_index, new_header)
                        if len(new_header) > len(df_toclear.columns):
                            new_header = new_header[0:len(df_toclear.columns)]
                            self.error_messages.append(
                                'Header is too long for columns left. Truncated')
                        self.parsed_tables.append(
                            (df_num, df_toclear, new_header, lead_index))

    def delete_empty_column(self, frame):
        columns_to_delete = []
        for i in frame.columns:
            if frame.dtypes.get(i) == float and len(frame[i].unique()) == 1:
                columns_to_delete.append(i)
        frame.drop(frame.columns[columns_to_delete], axis=1, inplace=True)
        return(frame)

    def delete_empty_row(self, frame):
        rows_to_delete = []
        for i in frame.index:
            found = False
            for j in frame.columns:
                if not (
                    (isinstance(
                        frame[j][i],
                        float) or isinstance(
                        frame[j][i],
                        np.float64)) and frame[j][i] != np.nan):
                    if len(
                        ((str(
                            frame[j][i]).replace(
                            u'\ufeff',
                            '')).replace(
                            u'\xa0',
                            '')).strip()) != 0:
                        found = True
            if not found:
                rows_to_delete.append(i)
        frame.drop(frame.index[rows_to_delete], inplace=True)
        return(frame)

    def modify_head(self, frame, check_words, column_index=0):
        delimeters_to_kill = ['В']
        head_rows = []
        suspected_rows = []
        for i in frame.index:
            found = False
            columns_to_analyse = [
                x for x in frame.columns if x != column_index]
            if (isinstance(frame[column_index][i], float) or isinstance(
                    frame[column_index][i], np.float64)) and np.isnan(frame[column_index][i]):
                head_rows.append(i)
            else:
                for j in columns_to_analyse:
                    if (isinstance(frame[j][i], float) or isinstance(
                            frame[j][i], np.float64)) and np.isnan(frame[j][i]):
                        t = 1
                    elif isinstance(frame[j][i], float) or isinstance(frame[j][i], np.float64):
                        t = 1
                    else:
                        str_to_search = str(frame[j][i]).lower()
                        for word in check_words:
                            if str_to_search.find(word.lower()) > -1:
                                found = True
                                break
                if not found and i == frame.index[0]:
                    str_to_search = str(frame[column_index][i]).lower()
                    for word in check_words:
                        if str_to_search.find(word.lower()) > -1:
                            found = True
                            suspected_rows.append(i)
                            break
                if found:
                    head_rows.append(i)
                else:
                    break

        if len(head_rows) == 0:
            return([], [])

        if len(head_rows) == len(suspected_rows):
            return([], [])

        column_to_delete = []
        column_names = np.empty(
            shape=(len(frame.columns),
                   len(head_rows)),
            dtype=object)
        column_codes = np.array(frame.columns)
        for i in range(len(frame.columns)):
            for j in range(len(head_rows)):
                column_names[i, j] = ''

        for i in range(len(frame.columns)):
            if column_codes[i] == column_index:
                column_names[i, 0] = 'Left_column'
            else:
                # проверка на кусок заголовка, сдвинутый влево, добавить потом
                # сеть!!
                check_rows = [x for x in frame.index if x not in head_rows]
                found = False
                for row in check_rows:
                    if (isinstance(frame[column_codes[i]][row], float) or isinstance(
                            frame[column_codes[i]][row], np.float64)) and np.isnan(frame[column_codes[i]][row]):
                        t = 1
                    elif str(frame[column_codes[i]][row]).strip() == '':
                        t = 1
                    elif str(frame[column_codes[i]][row]).strip() == ')':
                        t = 1
                        frame.at[row, column_codes[i - 1]
                                 ] = frame[column_codes[i - 1]][row] + ')'
                    elif str(frame[column_codes[i]][row]).strip() == '(2)':
                        t = 1
                    elif str(frame[column_codes[i]][row]).strip() == '(3)':
                        t = 1
                    elif str(frame[column_codes[i]][row]).strip() == '(4)':
                        t = 1
                    elif str(frame[column_codes[i]][row]).strip() == '$':
                        t = 1
                    else:
                        found = True
                        break
                if not found and i < len(frame.columns) - 1:
                    # пустая колонка
                    #next_column = column_codes[i+1]
                    for row in range(len(head_rows)):
                        if not((
                            isinstance(
                                frame[column_codes[i]][head_rows
                                                       [row]],
                                float)
                            or
                            isinstance(
                                frame[column_codes[i]][head_rows
                                                       [row]],
                                np.float64)) and np.isnan(
                                frame[column_codes[i]][head_rows[row]])):
                            # if not
                            # ((type(frame[column_codes[i]][head_rows[row]])==float
                            # or
                            # type(frame[column_codes[i]][head_rows[row]])==np.float64)
                            # and
                            # np.isnan(frame[column_codes[i]][head_rows[row]])):
                            column_names[i + 1, row] = column_names[i, row] + ' ' + str(
                                frame[column_codes[i]][head_rows[row]]) + ' ' + column_names[i + 1, row]
                        elif len(column_names[i, row]) > 0:
                            column_names[i + 1, row] = column_names[i,
                                                                    row] + column_names[i + 1, row]
                        column_to_delete.append(column_codes[i])
            # конец проверки
            for row in range(len(head_rows)):
                if not((
                    isinstance(
                        frame[column_codes[i]][head_rows[row]],
                        float)
                    or
                    isinstance(
                        frame[column_codes[i]][head_rows[row]],
                        np.float64)) and np.isnan(
                        frame[column_codes[i]][head_rows[row]])):
                    column_names[i, row] = column_names[i,
                                                        row] + ' ' + str(frame[column_codes[i]][head_rows[row]])

        # собираем колонки вместе и делаем итоговый список
        new_header = []
        for i in range(column_names.shape[0]):
            if column_codes[i] not in column_to_delete:
                cur_header = ''
                for j in range(column_names.shape[1]):
                    str_to_clear = str(column_names[i, j])
                    str_to_clear = (
                        str_to_clear.replace(
                            u'\ufeff',
                            '')).replace(
                        u'\xa0',
                        '')
                    for delimeter in delimeters_to_kill:
                        str_to_clear = str_to_clear.replace(delimeter, '')
                    cur_header = cur_header + ' ' + str_to_clear
                if len(cur_header.strip()) > 0:
                    new_header.append(cur_header.strip())
                elif i == 0:
                    new_header.append(cur_header.strip())

        frame.drop(head_rows, inplace=True)
        frame.drop(column_to_delete, axis=1, inplace=True)

        return(new_header, head_rows)

    def get_first_column(self, frame, left_column):
        left_words = []
        for row in frame.index:
            if not (
                (isinstance(
                    frame[left_column][row],
                    float) or isinstance(
                    frame[left_column][row],
                    np.float64)) and np.isnan(
                    frame[left_column][row])):
                left_words.append(
                    str(frame[left_column][row]).replace(u'\ufeff', ''))
        return(left_words)

    def clear_table(self, frame, left_column, header):
        frame.replace(np.nan, 19382)
        frame = frame.astype(str)
        frame.replace('19382', 'nan')

        delimeters_to_kill = ['*', '$', 'В', '(1)', '(2)', '(3)', '(4)', '(5)']
        rows_to_work = [x for x in frame.index]
        columns_to_work = [x for x in frame.columns]

        while True:
            correction_made = False
            for row in range(len(rows_to_work)):
                for column in range(len(columns_to_work)):
                    cur_string = frame[columns_to_work[column]][
                        rows_to_work[row]]
                    cur_string = cur_string.replace(u'\ufeff', '')
                    cur_string = cur_string.replace(u'\xa0', '')
                    cur_string = cur_string.strip()

                    if cur_string in delimeters_to_kill:
                        frame.at[rows_to_work[row],
                                 columns_to_work[column]] = 'nan'
                        correction_made = True
                    elif cur_string == '-':
                        frame.at[rows_to_work[row],
                                 columns_to_work[column]] = '0'
                        correction_made = True
                    elif cur_string == '—':
                        frame.at[rows_to_work[row],
                                 columns_to_work[column]] = '0'
                        correction_made = True
                    elif cur_string == ')':
                        if column > 0:
                            frame.at[rows_to_work[row], columns_to_work[column - 1]
                                     ] = frame[columns_to_work[column - 1]][rows_to_work[row]] + ')'
                            frame.at[rows_to_work[row],
                                     columns_to_work[column]] = 'nan'
                            correction_made = True
                        else:
                            frame.at[rows_to_work[row],
                                     columns_to_work[column]] = 'nan'
                            correction_made = True
                    elif cur_string == 'nan':
                        if column < len(columns_to_work) - 1:
                            next_string = frame[columns_to_work[column + 1]
                                                ][rows_to_work[row]]
                            next_string = next_string.replace(u'\ufeff', '')
                            next_string = next_string.replace(u'\xa0', '')
                            next_string = next_string.strip()
                            if next_string != 'nan':
                                frame.at[rows_to_work[row],
                                         columns_to_work[column]] = next_string
                                frame.at[rows_to_work[row],
                                         columns_to_work[column + 1]] = 'nan'
                                correction_made = True
            if not correction_made:
                break

        column_to_delete = []
        for column in columns_to_work:
            nan_counter = 0
            for row in rows_to_work:
                if frame[column][row] == 'nan':
                    nan_counter = nan_counter + 1
            if nan_counter == len(rows_to_work):
                column_to_delete.append(column)

        new_header = ['']
        for i in range(len(columns_to_work)):
            if columns_to_work[i] not in column_to_delete and i + 1 < len(
                    header):
                new_header.append(header[i + 1])
        frame.drop(column_to_delete, axis=1, inplace=True)
        return(frame, new_header)

    def find_best_row_column(self, phrase_list, filter_list, list_type):
        #list_type = 'columns' and 'rows'
        if len(filter_list) == 0:
            return(0, 0, False)
        years = []
        max_global = 0
        for i in range(len(filter_list)):
            found, max_year, is_single = self.if_year_in_string(
                phrase_list[filter_list[i]])
            years.append((i, found, max_year, is_single))
            if found and max_year > max_global:
                max_global = max_year
        if max_global == 0 and list_type == 'columns':
            return(filter_list[0], 0, False)
        if max_global == 0 and list_type == 'rows':
            return(filter_list[-1], 0, False)
        max_list = [x for x in years if x[2] == max_global]
        if len(max_list) == 1 and max_list[0][3]:
            return(filter_list[max_list[0][0]], max_list[0][2], True)
        else:
            if list_type == 'columns':
                return(filter_list[max_list[0][0]], max_list[0][2], False)
            else:
                return(filter_list[max_list[-1][0]], max_list[-1][2], False)

    def classify_list(self, phrase_list, word_to_code, max_len, rules, model):
        phrase_list_norm = self.prepare_words(phrase_list, rules)
        x_phrase = self.parse_phrase_without_y(
            phrase_list_norm, word_to_code, max_len)
        predicted = model.predict(x_phrase)
        y_phrase = [np.argmax(y, axis=-1, out=None) for y in predicted]
        return(y_phrase)

    def if_year_in_string(self, phrase):
        currentYear = datetime.now().year
        year_in_past = currentYear - 10
        years_found = []
        for i in range(len(phrase) - 3):
            if phrase[i].isdigit() and phrase[i +
                                              1].isdigit() and phrase[i +
                                                                      2].isdigit() and phrase[i +
                                                                                              3].isdigit():
                if int(
                        phrase[i:i + 4]) > year_in_past and int(phrase[i:i + 4]) < currentYear + 1:
                    years_found.append(int(phrase[i:i + 4]))
        if len(years_found) == 0:
            return(False, 0, False)
        if len(years_found) == 1:
            is_single = True
        else:
            is_single = False

        return(True, max(years_found), is_single)

    def load_files(self, model_path, dict_path):
        model_load_succesful = True
        dict_load_successful = True
        error_message = ''
        try:
            model = load_model(model_path)
        except Exception as e:
            error_message = str(e)
            model_load_succesful = False
        try:
            with open(dict_path, 'rb') as fp:
                param_array = pickle.load(fp)
                fp.close()
        except Exception as e:
            error_message = error_message + ' ' + str(e)
            dict_load_successful == False
        if not model_load_succesful or not dict_load_successful:
            return(False, error_message, [], [], 0, 0, [], '')
        max_features = param_array[0]
        max_len = param_array[1]
        all_words_stats = param_array[2]
        word_to_code = param_array[3]
        rules = param_array[4]

        check_words = [x[0] for x in all_words_stats]
        currentYear = datetime.now().year
        year_in_past = currentYear - 10
        for i in range(year_in_past, currentYear):
            check_words.append(str(i))

        return(True, '', check_words, word_to_code, max_features, max_len, all_words_stats, model, rules)

    def prepare_words(self, comb_list, rules):
        new_comb_corrected = []
        delimeters_to_remove = [
            '.',
            '(',
            ')',
            ',',
            '-',
            '/',
            '”',
            '’',
            '–',
            ':',
            '*',
            u'\ufeff',
            u'\xa0',
            '$']

        for i in comb_list:
            t_to_clear = ''.join([k for k in i if not k.isdigit()])
            for delimeter in delimeters_to_remove:
                t_to_clear = t_to_clear.replace(delimeter, ' ')
            t_space = t_to_clear
            for j in range(15):
                t_space = t_space.replace('  ', ' ')

            words_separated = t_space.split(' ')
            new_words_separated = []
            for word in words_separated:
                word_corrected = rules.get(word, word)
                if len(word_corrected) > 0 and word_corrected[0].isalpha(
                ) and word_corrected[0].islower():
                    word_corrected = word_corrected[0].upper(
                    ) + word_corrected[1:]
                t = re.findall('[A-Z][^A-Z]*', word_corrected)
                new_comb = []
                for k in t:
                    new_comb.append(rules.get(k, k))
                new_words_separated.append(' '.join(new_comb))

            new_word = ' '.join(new_words_separated)
            for j in range(10):
                new_word = new_word.replace('  ', ' ')
            new_word = new_word.strip()
            new_comb_corrected.append(new_word)
        return(new_comb_corrected)

    def parse_phrase_without_y(self, phrase_list, word_to_code, max_len):
        all_x = np.zeros((len(phrase_list), max_len))

        for i in range(len(phrase_list)):
            t = phrase_list[i].split(' ')
            length = 0
            j = 0
            while length < max_len and j < len(t):
                if len(t[j]) > 1:
                    all_x[i][length] = word_to_code.get(t[j], 1)
                    length = length + 1
                j = j + 1
        return(all_x)

    def get_execution_price(
            self,
            table,
            header,
            lead_index,
            word_to_code,
            max_len,
            rules,
            model):
        # 3 варианта: годы, сам показатель, ничего
        # 1 - найдена колонка, 2 - найден год, 3 - одна колонка с данными
        header_result = 0
        column_num = -1
        year_num_column = 0
        confidence_column = True
        y_header = self.classify_list(
            header, word_to_code, max_len, rules, model)

        y_header_filtered = []  # позиции с нужным классификатоом
        for i in range(len(y_header)):
            if y_header[i] == 6:
                y_header_filtered.append(i)
        if len(y_header_filtered) == 1:
            # вариант есть в заголовке и он один
            column_num = table.columns[y_header_filtered[0]]
            found, max_year, is_single = self.if_year_in_string(
                header[y_header_filtered[0]])
            if found:
                year_num_column = max_year
            header_result = 1
        elif len(y_header_filtered) > 1:
            best_index, year_num, confidence = self.find_best_row_column(
                header,
                y_header_filtered,
                'columns')
            column_num = table.columns[best_index]
            if year_num > 0:
                year_num_column = year_num
            if not confidence:
                confidence_column = False
            header_result = 1
        else:
            # ищем годы
            header_index = []
            for i in range(len(header)):
                header_index.append(i)
            best_index, year_num, confidence = self.find_best_row_column(
                header,
                header_index
                [1:],
                'columns')
            if year_num > 0:
                year_num_column = year_num
                column_num = table.columns[best_index]
                confidence_column = confidence
                header_result = 2
            else:
                if len(table.columns) == 2:
                    column_num = table.columns[header_index[1]]
                    header_result = 3
                else:
                    confidence_column = False
                    header_result = 4
        # теперь ищем строку
        column_rows = []
        row_result = 0
        row_num = -1
        year_num_row = 0
        confidence_row = True

        for row in table.index:
            column_rows.append(table[lead_index][row])
        y_column = self.classify_list(
            column_rows, word_to_code, max_len, rules, model)
        if header_result == 1 or header_result == 2 or header_result == 3:
            y_column_filtered = []  # позиции с нужным классификатоом
            for i in range(len(y_column)):
                if y_column[i] == 4:
                    y_column_filtered.append(i)
            if len(y_column_filtered) == 1:
                row_num = table.index[y_column_filtered[0]]
                found, max_year, is_single = self.if_year_in_string(
                    column_rows[y_column_filtered[0]])
                if found:
                    year_num_row = max_year
                row_result = 1
            elif len(y_column_filtered) > 1:
                best_index, year_num, confidence = self.find_best_row_column(
                    column_rows,
                    y_column_filtered,
                    'rows')
                row_num = table.index[best_index]
                if year_num > 0:
                    year_num_row = year_num
                if not confidence:
                    confidence_row = False
                row_result = 1
            else:
                # просто берем самое последнюю строку со значением
                for row in table.index:
                    if self.is_numeric(table[column_num][row]):
                        row_num = row
                if row_num == -1:
                    confidence_row = False
                    row_result = 4
                else:
                    row_result = 3
                    confidence_row = False
        else:  # вариант не ясного столбца
            y_column_filtered = []  # позиции с нужным классификатоом
            for i in range(len(y_column)):
                if y_column[i] == 4:
                    y_column_filtered.append(i)
            if len(y_column_filtered) == 1:
                row_num = table.index[y_column_filtered[0]]
                found, max_year, is_single = self.if_year_in_string(
                    column_rows[y_column_filtered[0]])
                if found:
                    year_num_row = max_year
                row_result = 1
                confidence_row = True
            elif len(y_column_filtered) > 1:
                best_index, year_num, confidence = self.find_best_row_column(
                    column_rows,
                    y_column_filtered,
                    'rows')
                row_num = table.index[best_index]
                if year_num > 0:
                    year_num_row = year_num
                if not confidence:
                    confidence_row = False
                row_result = 1
            else:
                confidence_row = False
                row_result = 4

        # результаты лучший вариант,  уверенность по строке, увереность по
        # колонке, год, если есть, уверенность
        best_year_num = 0
        best_year_confidence = False
        if header_result == 4 and row_result == 4:
            return(-1, -1, False, False, best_year_num, best_year_confidence)
        if year_num_column > 0 and confidence_column:
            best_year_num = year_num_column
            best_year_confidence = True
        elif year_num_row > 0 and confidence_row:
            best_year_num = year_num_row
            best_year_confidence = True
        elif year_num_column > 0 and not confidence_column:
            best_year_num = year_num_column
            best_year_confidence = False
        elif year_num_row > 0 and not confidence_row:
            best_year_num = year_num_row
            best_year_confidence = False
        # еще одна проверка года
        table_years_found, best_year_found_table, is_single = self.years_in_table(
            header, column_rows)
        if table_years_found and is_single:
            best_year_num = best_year_found_table
            best_year_confidence = True

        return(row_num, column_num, confidence_row, confidence_column, best_year_num, best_year_confidence)

    def years_in_table(self, header, column_rows):
        # to return found,year_num,is single
        single_year = True
        years_found = []
        for head in header:
            found, max_year, is_single = self.if_year_in_string(head)
            if not is_single:
                single_year = False
                break
            else:
                if found:
                    years_found.append(max_year)
        for row in column_rows:
            found, max_year, is_single = self.if_year_in_string(row)
            if not is_single:
                single_year = False
                break
            else:
                if found:
                    years_found.append(max_year)
        years_found = list(set(years_found))
        if single_year and len(years_found) == 1:
            return(True, years_found[0], True)
        elif len(years_found) > 0:
            return(True, max(years_found), False)
        else:
            return(False, 0, False)
