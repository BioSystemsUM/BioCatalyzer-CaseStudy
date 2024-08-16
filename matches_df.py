import pandas as pd


class MatchesDF:

    def __init__(self, matches_df_path: str, ms_data_path: str = None):
        self._matches_df = pd.read_csv(matches_df_path, sep='\t', header=0, compression='zip')
        if ms_data_path is not None:
            self._ms_data = pd.read_csv(ms_data_path, sep='\t', header=0, compression='zip')
        else:
            self._ms_data = None

    @property
    def matches_df(self):
        return self._matches_df

    @property
    def ms_data(self):
        return self._ms_data

    def get_matches_by_parent(self, parent: str):
        return self._matches_df[self._matches_df.ParentDrug == parent]

    def get_matches_by_index(self, index: int):
        return self._matches_df[self._matches_df.Index == index]

    def group_by_parent(self, sort: bool = True):
        mbp = self._matches_df.drop_duplicates(subset=['ParentCompound', 'Index'])
        mbp = mbp.groupby('ParentCompound').count()
        mbp = mbp[['Index']]
        msp = self._ms_data.groupby('ParentCompound').count()
        df = pd.concat([mbp, msp], axis=1, sort=False)
        df = df.iloc[:, :2]
        df.columns = ['Matches', 'Total']
        df['Percentage'] = df.Matches / df.Total
        if sort:
            return df.sort_values(by='Percentage', ascending=False)
        return df

    def group_by_mass_diff(self, sort: bool = True):
        mbmd = self._matches_df.drop_duplicates(subset=['ParentCompound', 'Index'])
        mbmd['MassDiff'] = [self._ms_data.iloc[i].MassDiff for i in mbmd.Index.values]
        mbmd = mbmd.groupby('MassDiff').count()
        mbmd = mbmd[['Index']]
        msp = self._ms_data.groupby('MassDiff').count()
        df = pd.concat([mbmd, msp], axis=1, sort=False)
        df = df.iloc[:, :2]
        df.columns = ['Matches', 'Total']
        df['Percentage'] = df.Matches / df.Total
        if sort:
            return df.sort_values(by='Percentage', ascending=False)
        return df


if __name__ == '__main__':
    matches_df = MatchesDF('results/round1/matches.tsv',
                           'data/ms_data.tsv')
    print(matches_df.group_by_mass_diff())
