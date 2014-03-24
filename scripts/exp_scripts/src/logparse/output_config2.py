
top_dir = '/home/abhigyan/gnrs/results/'
plot_files_dir = '/home/abhigyan/gnrs/plot_files2/'

output_dir = top_dir + 'jan24/microbenchmark'


schemes = ['Auspice',  'Uniform', 'Locality', 'FixedTimeout', 'NoLoadBalance', 'NoLoadBalanceFixedTimeout']

scheme_dirs = {}

scheme_dirs['Auspice'] = top_dir + 'jan24/v1_full_2_stats'
scheme_dirs['Uniform'] = top_dir + 'jan24/v4_uniform_stats'
scheme_dirs['NoLoadBalance'] = top_dir + 'jan24/v2_noloadbalance_2_stats'
scheme_dirs['FixedTimeout'] = top_dir + 'jan24/v3_fixedtimeout_stats'
scheme_dirs['Locality'] = top_dir + 'jan24/v5_locality_4_stats'
scheme_dirs['NoLoadBalanceFixedTimeout'] = top_dir + 'jan24/v6_noloadbalance_fixedtimeout_stats'

plot_names = {}

plot_names['Auspice'] = 'Auspice'
plot_names['Uniform'] = 'Random'
plot_names['NoLoadBalance'] = '"NoLoadBal"'
plot_names['FixedTimeout'] = 'FixedTimout'
plot_names['Locality'] = 'Locality'
plot_names['NoLoadBalanceFixedTimeout'] = '"NoLoadBal-FixedTimout"'
