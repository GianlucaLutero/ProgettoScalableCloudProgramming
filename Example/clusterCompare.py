import pandas


root = "C:\\Users\\Gianluca\\Desktop\\result"
# Risultato del clustering con kmeans con k = 4
cluster = pandas.read_csv(root+"\\cluster_count.csv\\cluster_count.csv")

# Carico i cluster esatti
por_c = pandas.read_csv(root+"\\correct_position_0.csv\\correct_position_0.csv")
dif_c = pandas.read_csv(root+"\\correct_position_1.csv\\correct_position_1.csv")
cen_c = pandas.read_csv(root+"\\correct_position_2.csv\\correct_position_2.csv")
att_c = pandas.read_csv(root+"\\correct_position_3.csv\\correct_position_3.csv")

control_list = [att_c,por_c,cen_c,dif_c]
cluster_names = ["Attaccanti","Portieri","Centrocampisti","Difensori"]

#print(dif_c)
#print(por_c)
#print(att_c)
#print(cen_c)


dif_k = cluster[cluster.prediction == 0]

#for x in range(0,4):
#	print(cluster[cluster.prediction == x])
	

#print(dif_c)

control_total = 0
analysis_total = 0

for x in dif_c.itertuples():
	tmp = x.Position
	tmp_dif = dif_k.loc[dif_k['Position'] == tmp,'count'].values
	#print(tmp_dif[0])
	analysis_total += tmp_dif
	control_total += x.count
	#print(x)


print("Test totale")
i = 0	

for ctrl in control_list:


	control_total = 0
	analysis_total = 0

	for x in ctrl.itertuples():
		tmp = x.Position
		tmp_dif = cluster[cluster.prediction == i].loc[cluster['Position'] == tmp,'count'].values
		#print(tmp_dif[0])
		analysis_total += tmp_dif[0]
		control_total += x.count

	print("Precisione per il cluster: "+cluster_names[i])		
	i = i + 1
	print(analysis_total/control_total)	



#print("Success ratio")
#print(analysis_total/control_total)	