import argparse
import glob
import pandas as pd

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--result_path', type=str, default='./results',
                        help='The output folder path.')
    args = parser.parse_args()

    output = []
    for feature in ['line', 'point', 'polygon']:
        file = glob.glob(f'{args.result_path}/{feature}_features_*/*.csv')
        if len(file) == 1:
            data = pd.read_csv(file[0])
            output.append(data)
    output = pd.concat(output)
    output.to_csv(f'{args.result_path}/geographic_features.csv', header=True, index=False)
