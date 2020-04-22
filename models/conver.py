import os
from tensorflow.keras.models import load_model  # type: ignore


def main():
    for root, _, filenames in os.walk(
            os.path.dirname(os.path.abspath(__file__))):
        for filename in filenames:
            if filename.endswith('_old.h5'):
                continue
            if not filename.endswith('.h5'):
                continue

            model = load_model(
                os.path.join(root, filename))
            os.rename(
                os.path.join(root, filename),
                os.path.join(root, filename[:-3] + '_old.h5'))
            model.save(
                os.path.join(root, filename))


if __name__ == '__main__':
    main()
