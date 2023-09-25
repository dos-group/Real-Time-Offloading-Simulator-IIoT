# 50 fast clients:

1. Delay 5ms Jitter 0ms
- Worst Fit
Tasks accepted:		572/776 0.74
Deadlines met:		572/572 1.00

Worker efb2cddb Utilization:	0.86 (280 tasks)
Worker 76bcba42 Utilization:	0.91 (294 tasks)
Total Worker Utilization:	0.89 (574 tasks)

2. Delay 20ms Jitter 10ms
- Worst Fit
Tasks accepted:		507/770 0.66
Deadlines met:		502/507 0.99

Worker 26b2133c Utilization:	0.82 (265 tasks)
Worker 564df636 Utilization:	0.76 (245 tasks)
Total Worker Utilization:	0.79 (510 tasks)

- First Fit
Tasks accepted:		485/774 0.63
Deadlines met:		480/485 0.99

Worker 848d6a83 Utilization:	0.68 (218 tasks)
Worker 88428c31 Utilization:	0.83 (268 tasks)
Total Worker Utilization:	0.75 (486 tasks)

- Best Fit
Tasks accepted:		473/773 0.61
Deadlines met:		468/473 0.99

Worker 8ae446cd Utilization:	0.68 (219 tasks)
Worker 293284fb Utilization:	0.79 (256 tasks)


3. Delay 20ms Jitter 0ms
- Worst Fit
Tasks accepted:		517/772 0.67
Deadlines met:		517/517 1.00

Worker 300e2b5d Utilization:	0.78 (251 tasks)
Worker 8b8bbb97 Utilization:	0.83 (269 tasks)
Total Worker Utilization:	0.80 (520 tasks)



First Fit vs Worst Fit
poetry run python parse_logs.py all_clients logs/
Tasks accepted:		142/174 0.82
Deadlines met:		142/142 1.00
poetry run python parse_logs.py all_workers logs/
Worker ad0b19e8 Utilization:	0.76 (64 tasks)
Worker 1b2ccbd2 Utilization:	1.00 (80 tasks)
Total Worker Utilization:	0.88 (144 tasks)
poetry run python parse_logs.py all_clients logs/
Tasks accepted:		168/180 0.93
Deadlines met:		168/168 1.00
poetry run python parse_logs.py all_workers logs/
Worker fca61be3 Utilization:	1.00 (98 tasks)
Worker 7a21d92c Utilization:	0.96 (73 tasks)
Total Worker Utilization:	0.98 (171 tasks)


FF:
Tasks accepted:		147/182 0.81
Deadlines met:		146/147 0.99
Worker 8b9bbd91 Utilization:	0.98 (79 tasks)
Worker 63667146 Utilization:	0.74 (70 tasks)
Total Worker Utilization:	0.86 (149 tasks)

WF:
Tasks accepted:		156/193 0.81
Deadlines met:		156/156 1.00
Worker 6f4dd907 Utilization:	0.90 (74 tasks)
Worker cad402f1 Utilization:	0.94 (85 tasks)
Total Worker Utilization:	0.92 (159 tasks)