import os

import matplotlib.pyplot as plt
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
from mpl_toolkits.axes_grid1.inset_locator import mark_inset

def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

flag = 0
for i in os.listdir("/Users/fariakalim/Desktop/Research/Stela++/29thResults/HengeResults/Results/"):
    if i.endswith(".log") :
        f = open("/Users/fariakalim/Desktop/Research/Stela++/29thResults/HengeResults/Results/"+i, 'r')
        filename = f.__getattribute__("name").split(".")[0]
        print filename
        topology1 = []
        topology2 = []
        topology3 = []
        topology4 = []
        input_at_source_1 = []
        output_at_sink_1 = []
        input_at_source_2 = []
        output_at_sink_2 = []
        input_at_source_3 = []
        output_at_sink_3 = []
        input_at_source_4 = []
        output_at_sink_4 = []
        time = []
        target = []
        target_operator = []
        victim_operator = []
        victim = []
        rebalance_time = []
        for line in f:

                line = line.split("\n")[0]
                one_line = line.split(',')

                if "/var/nimbus/storm" not in one_line[0] and len(one_line) > 1:

                    if "production-topology1"  in one_line[0]:
                        topology1.append(float(one_line[2]))
                        input_at_source_1.append(float(one_line[3]))
                        output_at_sink_1.append(float(one_line[4]))
                    elif "production-topology2"  in one_line[0]:
                        topology2.append(float(one_line[2]))
                        time.append(float(one_line[5]))
                        input_at_source_2.append(float(one_line[3]))
                        output_at_sink_2.append(float(one_line[4]))
                    elif "production-topology3"  in one_line[0]:
                        topology3.append(float(one_line[2]))
                        input_at_source_3.append(float(one_line[3]))
                        output_at_sink_3.append(float(one_line[4]))
                    elif "production-topology4"  in one_line[0]:
                        topology4.append(float(one_line[2]))
                        input_at_source_4.append(float(one_line[3]))
                        output_at_sink_4.append(float(one_line[4]))
                    elif len(one_line) == 1 and "Running" not in one_line[0] and is_number(one_line[0]):
                        rebalance_time.append(float(one_line[0]))

                elif "/var/nimbus/storm" in one_line[0] :
                    time_for_rebalance = line.split(" ")
                    if "Running" not in line:
                        if flag == 0:
                            target.append(time_for_rebalance[2])
                            target_operator.append(time_for_rebalance[4])
                            flag = 1
                        else:
                            victim.append(time_for_rebalance[2])
                            victim_operator.append(time_for_rebalance[4])
                            flag = 0
                elif len(one_line) == 1 and is_number(one_line[0]):
                    rebalance_time.append(float(one_line[0]))

        val = time[0]

        for i in range(0, len(time)):
            time[i] = (time[i] - val)/1000

        for i in range(0, len(rebalance_time)):
            rebalance_time[i] = (rebalance_time[i] - val)/1000

        min_length = len(time)
        if min_length > len(topology1):
            min_length = len(topology1)
        if min_length > len(topology2):
            min_length = len(topology2)
        if min_length > len(topology3):
            min_length = len(topology3)
        if min_length > len(topology4):
            min_length = len(topology4)


        time = time[0:min_length]
        topology1 = topology1 [0:min_length]
        topology2 = topology2 [0:min_length]
        topology3 = topology3 [0:min_length]
        topology4 = topology4 [0:min_length]
        input_at_source_1 = input_at_source_1[0:min_length]
        output_at_sink_1 = output_at_sink_1[0:min_length]
        input_at_source_2 = input_at_source_2[0:min_length]
        output_at_sink_2 = output_at_sink_2[0:min_length]
        input_at_source_3 = input_at_source_3[0:min_length]
        output_at_sink_3 = output_at_sink_3[0:min_length]
        input_at_source_4 = input_at_source_4[0:min_length]
        output_at_sink_4 = output_at_sink_4[0:min_length]

        fig, ax = plt.subplots()
        ax.scatter(time,  topology1, edgecolors = "blue", label= "T1 SLO=1", marker = "D", facecolors='none', s=40,)
        ax.scatter(time, topology2, edgecolors = "green", label= "T2 SLO=0.8", marker = ">", facecolors='none',s=40,)
        ax.scatter(time, topology3, edgecolors = "red", label= "T3 SLO=1", marker="h", facecolors='none', s=40,)
        ax.scatter(time, topology4, edgecolors = "darkorange", label= "T4 SLO=0.8",  marker="s", facecolors='none',s=40,)


        ax.set_xlabel('Time/S', fontsize=10)
        ax.set_ylabel('Juice', fontsize=10)
        #ax.set_title(filename.split("/")[len(filename.split("/"))-1])

        ax.grid(True)
        fig.tight_layout()

        plt.vlines(x=600, ymax=5 , ymin=-1, label="Ten Minute Mark", colors='blue')


        linestyles = [ '--' , '--' , 'solid', 'dotted']
        for j in range(0, len(rebalance_time)):
            la = target[j] + " " + target_operator[j] + " " + victim[j] + " " + victim_operator[j]
            plt.vlines(x=rebalance_time[j], ymax=5 , ymin=-1,  colors='black', linestyle=linestyles[j%4]) #label=la,label="rebalance "+str(j+1),label="rebalance "+str(j+1),
        plt.legend(loc=9, bbox_to_anchor=(0.5, -0.1), ncol=5,prop={'size':10})
        plt.xlim(-10,1900)
        plt.ylim(-0.01,4)


        axins = zoomed_inset_axes(ax,  2.5, loc=1)
        axins.scatter(time, topology1, edgecolors = "blue", label= "T1", marker = "D", facecolors='none',s=40,)
        axins.scatter(time, topology2, edgecolors = "green", label= "T2", marker = ">", facecolors='none',s=40,)
        axins.scatter(time, topology3, edgecolors = "red", label= "T3", marker="h", facecolors='none', s=40,)
        axins.scatter(time, topology4, edgecolors = "darkorange", label= "T4",  marker="s", facecolors='none', s=40,)
        axins.vlines(x=600, ymax=5 , ymin=-1, label="Ten Minute Mark", colors='blue')

        for j in range(0, len(rebalance_time)):
            la = target[j] + " " + target_operator[j] + " " + victim[j] + " " + victim_operator[j]
            axins.vlines(x=rebalance_time[j], ymax=5 , ymin=-1,  colors='black', linestyle=linestyles[j%4]) #label=la,label="rebalance " + str(j+1),


        x1, x2, y1, y2 = 590, 1300, 0.8, 1.2 # specify the limits
        axins.set_xlim(x1, x2) # apply the x-limits
        axins.set_ylim(y1, y2) # apply the y-limits
        mark_inset(ax, axins, loc1=3, loc2=4, fc="none", ec="0.5")


        plt.savefig(filename+'.png', bbox_inches='tight')


        fig, ax = plt.subplots()
        ax.scatter(time, input_at_source_1, edgecolors = "chocolate", label= "T1 input", facecolor='none', marker = "D", s=40)
        ax.scatter(time, output_at_sink_1, edgecolors = "black", label= "T1 output" ,facecolor='none', marker = "+", s=40)
        ax.scatter(time, input_at_source_2, edgecolors = "red", label= "T2 input",facecolor='none', marker = ">", s=40)
        ax.scatter(time, output_at_sink_2, edgecolors = "blue", label= "T2 output" ,facecolor='none', marker = "<",s=40)
        ax.scatter(time, input_at_source_3, edgecolors = "teal", label= "T3 input", facecolor='none', marker = "x",s=40)
        ax.scatter(time, output_at_sink_3, edgecolors = "green", label= "T3 output", facecolor='none', marker = "h", s=40)
        ax.scatter(time, input_at_source_4, edgecolors = "purple", label= "T4 input", facecolor='none',marker = "v" , s=40)
        ax.scatter(time, output_at_sink_4, edgecolors = "magenta", label= "T4 output" , facecolor='none', marker = "s", s=40)


        for j in range(0, len(rebalance_time)):
            la = target[j] + " " + target_operator[j] + " " + victim[j] + " " + victim_operator[j]
            ax.vlines(x=rebalance_time[j], ymax=5000 , ymin=-1,  colors='black', linestyle=linestyles[j%4]) #label=la,label="rebalance " + str(j+1),

        ax.set_xlabel('Time/S', fontsize=10)
        ax.set_ylabel('Number of Tuples', fontsize=10)



        ax.grid(True)
        fig.tight_layout()
        plt.vlines(x=600, ymax=5000 , ymin=-1, label="Ten Minute Mark", colors='blue')
        plt.legend(loc=9, bbox_to_anchor=(0.5, -0.1), ncol=5,prop={'size':10})
        plt.xlim(-10,1900)
        plt.ylim(-1,5000)
        plt.savefig(filename+"+tuples"+'.png', bbox_inches='tight')
