dot3StatsTable = u'.1.3.6.1.2.1.10.7.2.1'
dot3StatsAlignmentErrors = dot3StatsTable + u'.2'
dot3StatsFCSErrors = dot3StatsTable + u'.3'
dot3StatsFrameTooLongs = dot3StatsTable + u'.13'

dots3stats_table_oids = [dot3StatsFCSErrors, dot3StatsAlignmentErrors, dot3StatsFrameTooLongs]
