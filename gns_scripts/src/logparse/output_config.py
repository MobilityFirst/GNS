top_dir = '/home/abhigyan/gnrs/results/'
plot_files_dir = '/home/abhigyan/gnrs/plot_files/'

output_dir = top_dir + 'jan21/comparison'


schemes = ['LOCALITY',  'CODONS', 'STATIC3', 'REPLICATEALL']
scheme_dirs = {}
scheme_dirs['LOCALITY'] = top_dir + 'jan19/locality_alpha0.2_stats'
scheme_dirs['CODONS'] = top_dir + 'jan21/beehive_load10_lb_newNSchoose_stats'
scheme_dirs['STATIC3'] = top_dir + 'jan19/static3_phi2_stats'
scheme_dirs['REPLICATEALL'] = top_dir + 'jan18/replicate_all_updatelog_stats'

#scheme_dirs['LOCALITY'] = top_dir + 'jan26/locality_load20_stats'
#scheme_dirs['CODONS'] = top_dir + 'jan21/beehive_load10_lb_newNSchoose_stats'
#scheme_dirs['STATIC3'] = top_dir + 'jan19/static3_phi2_stats'
#scheme_dirs['REPLICATEALL'] = top_dir + 'jan26/replicateall_load20_stats'


plot_names = {}

plot_names['LOCALITY'] = 'Auspice'
#plot_names['UNIFORM'] = 'Uniform'
plot_names['CODONS'] = 'Codons'
plot_names['STATIC3'] = 'Static3'
plot_names['REPLICATEALL'] = 'ReplicateAll'
