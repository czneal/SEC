import os
import pandas as pd

import indi.options_lib as olib

from utils import Settings
from mysqlio.readers import MySQLReader


def main():
    parcer = olib.Option_parser(
        model_path=os.path.join(
            Settings.models_dir(),
            'option_tables_classifier_20190822.h5'),
        dict_path=os.path.join(
            Settings.models_dir(),
            'option_tables_data.pkl'))

    print(parcer.state)  # в норме 0
    print(parcer.error_messages)

    r = MySQLReader()
    data = r.fetch("""select * from text_blocks \
    where tag = 'ScheduleOfShareBasedCompensationActivityTableTextBlock' \
    limit 10""", [])
    r.close()

    for row in data:
        try:
            print(f"adsh: {row['adsh']}")
            html_string = row['text_block']
            frames = pd.read_html(html_string)

            parcer.parse_html(html_string)
            print(parcer.state)  # в норме 0
            print(parcer.error_messages)

            for table_num in range(len(parcer.parsed_tables)):
                str_found, best_year_num, best_year_confidence = parcer.get_option_exercise_price(
                    table_num)
                print('Exercise price is ' + str_found)
                print(best_year_num)
                print(best_year_confidence)

            print(parcer.error_messages)
            parcer.clear_history()
        except Exception as e:
            print(str(e))


if __name__ == '__main__':
    main()
