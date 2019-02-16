import numpy as np
import matplotlib.pyplot as plt


months = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']


def make_weekly_graph(hemato):
    '''
    # Internações por dia da semana, em cada mês
    # (planejamento de plantões/sobreavisos)
    # não é a prevalência, é o número de novas internações (incidência)
    # TODO: fazer bootstrap para ver significância, fazer prevalência

    '''
    pivoted = hemato.pivot_table('N_AIH', index=hemato.DT_INTER.dt.weekday, columns=hemato.DT_INTER.dt.month,
                                 aggfunc='count')

    pivoted.columns.name = None
    pivoted.columns = months
    pivoted.index.name = None

    pivoted.plot(figsize=(12, 8), legend=True)
    plt.xticks(np.arange(7), ['Dom', 'Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb'])
    plt.ylabel('Número de internações')
    plt.title('Internações por dia da semana a cada mês:')
    plt.show()
    print('Parece que não muda muito o padrão.')


def make_daily_int_each_month(hemato):

    pivoted = hemato.pivot_table('N_AIH', index=hemato.DT_INTER.dt.day, columns=hemato.DT_INTER.dt.month,
                                 aggfunc='count')

    pivoted.columns.name = None
    pivoted.columns = months
    pivoted.index.name = None

    pivoted.plot(figsize=(12, 8), legend=True, alpha=.9,
                 xticks=(np.arange(1, 32)), yticks=(np.arange(4601, step=500)), ylim=(0, 3000))
    plt.title('Internações totais por dia do mês:')
    plt.ylabel('Número de internações')
    plt.xlabel('Dia do mês')
    plt.show()
