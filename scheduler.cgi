#!/usr//bin/python
#Queue scheduler for SALT (web interface)
#Our main goal is to maximise number of P1 and P2 blocks ; P0 are very high priority and P3 are filler; 
#P4 are to be done only if nothing else is doable; 

#DB API to access SALT science database 
import MySQLdb

import re,glob,os,sys
#used to allow for some cgi functionality
import cgi
import cgitb
cgitb.enable()

#matplotlib needs this to create the plot properly
os.environ['HOME'] = '/tmp/'
import matplotlib
matplotlib.use('Agg')

form = cgi.FieldStorage()

#Scheduler classes
from SubBlock import *
from Queue import *

#needed to prep the matplotlib plot
import cStringIO
import numpy as np
import random

#A few useful matplotlib things
from matplotlib import pyplot as plt
from matplotlib.patches import Rectangle

#Date handling for matplotlib and classes
from matplotlib import dates
import matplotlib.dates as md
from datetime import datetime, timedelta

#Module that helps with time/date conversion and moon times
#http://rhodesmill.org/pyephem/
import ephem


#setup some global things
#degrees to radians
dtor = np.pi/180.0
#figuresize for matplotlib
#fsize=[20,40]
fsize=[8,8]
#yrange for matplotlib plot
ymin=-0.3
ymax=5.3
#params for x-axis labels
XLAB_ROT=40
XLAB_FSIZE=14
#used for some debugging in the matplotlib
SHOW_FULL=0

#This is not used in this page, but it's kept here
#to show how we calculate whether a block with start and end times Astart,Aend
#overlaps a block with times Bstart,Bend

#def IsOverlapping(Astart,Aend,Bstart,Bend):
#   return ( max(Astart,Bstart) <= min(Aend,Bend))


#Not longer in use, but can be used by matplotlib interactive plots to launch browser (macosx)
#def onpick(event):
#   if isinstance(event.artist, Rectangle):
#      thisrect = event.artist
#      #open -a /Applications/Firefox.app
#      url = "open -b org.mozilla.firefox " + str(thisrect.get_url())
#      #webbrowser.open_new_tab(url)
#      #print "rect selected: %s" % url
#      os.system(url)

#Setup default cgi params 
#Current semester
cgi_propcode = '2014-2'
cgi_piname = ''
cgi_blockid= ''
#minimum distance from moon
cgi_moondist = 30.0
#niter roughly controls how much work the randomisation does; increase to do more thorough (slower) job
cgi_niter = 10 
cgi_ptreat = 'seq'

#IsDefault is true if there are no cgi params passed to the page
#This occurs when you first load up the page

IsDefault = len(cgi.parse_qs(os.environ['QUERY_STRING'])) == 0
if (IsDefault):
    #Get the current time
    now = datetime.utcnow()+timedelta(hours=2)
    #Wrap the date in case you run it at night after midnight
    if(now.hour >= 0 and now.hour <= 8):
        now = now-timedelta(days=1)
    #format the dates as appropriate using strftime for use in the forms
    cgi_date = now.strftime("%Y/%m/%d")
    cgi_alttime = now.strftime("%H:%M")
    cgi_altendtime = now.strftime("%H:%M")
    #min and max seeing
    cgi_smin = "0" 
    cgi_smax = "10" 
    #transparency
    cgi_tran = "any" 

#Retrieve params if they are specified in the cgi params
if "date" in form:
    cgi_date = form.getvalue('date')
    cgi_alttime = form.getvalue('alttime')
    cgi_altendtime = form.getvalue('altendtime')

if "propcode" in form:
    cgi_propcode = form.getvalue('propcode')

if "piname" in form:
    cgi_piname = form.getvalue('piname')

if "blockid" in form:
    cgi_blockid = form.getvalue('blockid')

if "moondist" in form:
    cgi_moondist = form.getvalue('moondist')

if "niter" in form:
    cgi_niter = form.getvalue('niter')

if "ptreat" in form:
    cgi_ptreat = form.getvalue('ptreat')

if "smin" in form:
    cgi_smin = form.getvalue('smin')

if "smax" in form:
    cgi_smax = form.getvalue('smax')

if "tran" in form:
    cgi_tran = form.getvalue('tran')

#checkboxes for a few custom options

#ignore moon for time critical observations
cgi_notcmoon = 0
if (IsDefault):
    cgi_notcmoon = 0
if "notcmoon" in form:
    cgi_notcmoon = 1

#use alternate start time for the queue
cgi_usealttime = 0
if (IsDefault):
    cgi_usealttime = 0
if "usealttime" in form:
    cgi_usealttime = 1

#use alternate end time for the queue
cgi_usealtendtime = 0
if (IsDefault):
    cgi_usealtendtime = 0
if "usealtendtime" in form:
    cgi_usealtendtime = 1


#instrument checkboxes
#TODO: Add separate modes here too
cgi_rss = 0
if (IsDefault):
    cgi_rss = 1
if "RSS" in form:
    cgi_rss = 1

cgi_scam = 0
if (IsDefault):
    cgi_scam = 1
if "SCAM" in form:
    cgi_scam = 1

cgi_hrs = 0
if (IsDefault):
    cgi_hrs = 1
if "HRS" in form:
    cgi_hrs = 1

cgi_bvit = 0
if (IsDefault):
    cgi_bvit = 1
if "BVIT" in form:
    cgi_bvit = 1

#Target priority checkboxes
cgi_p0 = 0
if (IsDefault):
    cgi_p0 = 1
if "p0" in form:
    cgi_p0 = 1

cgi_p1 = 0
if (IsDefault):
    cgi_p1 = 1
if "p1" in form:
    cgi_p1 = 1

cgi_p2 = 0
if (IsDefault):
    cgi_p2 = 1
if "p2" in form:
    cgi_p2 = 1

cgi_p3 = 0
if (IsDefault):
    cgi_p3 = 1
if "p3" in form:
    cgi_p3 = 1

cgi_p4 = 0
if (IsDefault):
    cgi_p4 = 0
if "p4" in form:
    cgi_p4 = 1

#create a list to pass to the queue class recording what priorties were selected
priority_list = []
if(cgi_p0):
    priority_list.append(0)
if(cgi_p1):
    priority_list.append(1)
if(cgi_p2):
    priority_list.append(2)
if(cgi_p3):
    priority_list.append(3)
if(cgi_p4):
    priority_list.append(4)

#define the seeing range for the queue
seeing_range = [cgi_smin,cgi_smax]

#create a list to pass to the queue class recording what instruments were selected
instrument_list = []
if(cgi_rss):
   instrument_list.append("RSS")
if(cgi_scam):
   instrument_list.append("SCAM")
if(cgi_hrs):
   instrument_list.append("HRS")
if(cgi_bvit):
   instrument_list.append("BVIT")

db = "sdb.salt"
debug = 0
#create an instance of the Queue class
q = Queue(cgi_date,priority_list,seeing_range,cgi_usealttime,cgi_alttime,cgi_usealtendtime,cgi_altendtime,instrument_list,cgi_tran,cgi_notcmoon,cgi_propcode,cgi_piname,cgi_blockid,cgi_moondist,db,debug)
#get the twilight start and end times (read from NightInfo in the science database within the Queue class)
(dstart,dend) = q.GetTwilightTimes()
#get the night duration in hours
total_duration = q.GetDuration()
duration_str="%.2f h" % (total_duration)
#Interval to display for the night with some padding at the edges
x=[dstart-timedelta(minutes=30),dend+timedelta(minutes=30)]
#Divide it up into 3 separate chunks to display with an equal timescale in each subplot
dt=timedelta(hours=total_duration*1.0/3.0)

#twilight buffer
dtwi=timedelta(minutes=30)
#buffer between plots (there is a small amount of information displayed twice for continuity)
db=timedelta(minutes=5)

#Setup the start and end times for each subplot
#first = x1
#second = x2
#third = x3
t1=dstart-dtwi
t2=t1+dtwi+dt
x1=[t1,t2]
t1=t2
t2=t1+dt
x2=[t1-db,t2+db]
t1=t2
t3=dend+dtwi
x3=[t1,t3]

#Define the yrange of the plot (using global values above)
#and the 
yrange=[ymin,ymax]

#Create the figure
f, (ax1, ax2, ax3) = plt.subplots(nrows=3,ncols=1, sharey=True,subplot_kw=dict(ylim=yrange,yticks=[]),figsize=fsize)
#Adjust the size boundaries
f.subplots_adjust(hspace=0.3,left=0.05,right=0.95,top=0.925)

#Defines the format of the X-axis (hours and minutes only)
xfmt=md.DateFormatter('%H:%M')

#Set the x-axis format, time range and major ticks/labels at every hour
ax1.xaxis.set_major_formatter(xfmt)
ax2.xaxis.set_major_formatter(xfmt)
ax3.xaxis.set_major_formatter(xfmt)
ax1.set_xlim(x1)
ax2.set_xlim(x2)
ax3.set_xlim(x3)
ax1.xaxis.set_major_locator(md.HourLocator(interval=1))
ax2.xaxis.set_major_locator(md.HourLocator(interval=1))
ax3.xaxis.set_major_locator(md.HourLocator(interval=1))

#Add sub-ticks at every 10 minutes (not labelled)
ax1.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))
ax2.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))
ax3.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))

#setup length of the tick marks
ax1.tick_params(which='major',length=7,width=2)
ax1.tick_params(which='minor',length=5)
ax2.tick_params(which='major',length=7,width=2)
ax2.tick_params(which='minor',length=5)
ax3.tick_params(which='major',length=7,width=2)
ax3.tick_params(which='minor',length=5)

#Retrieve the moon illumination fraction to display on plot
#(taken from NightInfo in science database as retrieved within the Queue class)
moonstr = ' [%d per cent]' % (q.GetMoonIllum())
#Set the title with the date, night duration in hours and moon illumination fraction 
ax1.set_title(dstart.strftime("%d %b %Y")+"\n"+duration_str+moonstr)#,weight='bold',va='top')

#format the axes properly
plt.setp(ax1.get_yticklabels(), visible=False)
plt.setp(ax2.get_yticklabels(), visible=False)
plt.setp(ax3.get_yticklabels(), visible=False)
for label in ax1.xaxis.get_ticklabels():
   label.set_rotation(XLAB_ROT)
   label.set_fontsize(XLAB_FSIZE)
for label in ax2.xaxis.get_ticklabels():
   label.set_rotation(XLAB_ROT)
   label.set_fontsize(XLAB_FSIZE)
for label in ax3.xaxis.get_ticklabels():
   label.set_rotation(XLAB_ROT)
   label.set_fontsize(XLAB_FSIZE)

#plot with red dashed lines the official twilight times
ax1.plot([dstart,dstart],yrange,'r--',lw=2.0)
ax3.plot([dend,dend],yrange,'r--',lw=2.0)

#calculate buffer times either side of twilight 
ti5=dstart-timedelta(minutes=15)
ti10=dstart-timedelta(minutes=20)
ti15=dstart-timedelta(minutes=25)
tf5=dend+timedelta(minutes=5)
tf10=dend+timedelta(minutes=10)
tf15=dend+timedelta(minutes=15)

#display these buffer times with lines corresponding to their limits
#black = dark, grey = grey, yellow = bright
if(cgi_usealtendtime == 0):
   ax3.plot([tf5,tf5],yrange,'k-',lw=2.0)
   ax3.plot([tf10,tf10],yrange,'-',lw=2.0,color='#808080')
   ax3.plot([tf15,tf15],yrange,'y-',lw=2.0)
if(cgi_usealttime == 0):
    ax1.plot([ti5,ti5],yrange,'k-',lw=2.0)
    ax1.plot([ti10,ti10],yrange,'-',lw=2.0,color='#808080')
    ax1.plot([ti15,ti15],yrange,'y-',lw=2.0)


#Alpha value for moon background colouring
moon_alpha = 0.3

#Get the moon start and end times
#Create a rectangle for each sub-plot and add it to the plot
(mstart,mend,mduration) = q.GetMoonRange()
#If there is a rise or set time for the moon, it gets displayed
#Otherwise it rises/sets outside the twilight times
if(mstart != 'NULL' and mend != 'NULL'):
   ax1.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))
   ax2.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))
   ax3.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))


#This ensures we don't try and optimise the schedule when loading the page without params specified
if(not IsDefault):
	#This is a placeholder method for optimising the queue
	#Sequential allocates more higher priority blocks 
	cgi_niter = int(cgi_niter)
	if(cgi_ptreat== "seq"):
	   #Treat priorities sequentially
	   if(cgi_p0 and cgi_p1):
	      q.RandomiseBlocks(cgi_niter,False,0,1)
	   if(cgi_p1 and cgi_p2):
	      q.RandomiseBlocks(cgi_niter,False,1,2)
	   if(cgi_p2 and cgi_p3):
	      q.RandomiseBlocks(cgi_niter,False,2,3)
	   if(cgi_p3 and cgi_p4):
	      q.RandomiseBlocks(cgi_niter,False,3,4)
	else:
	   #Treat all priorities together
	   q.RandomiseBlocks(cgi_niter,True,0,0)

#This interfaces with the Queue class to add all active blocks to the plot
#The function creates several Rectangles and adds them to each sub-plot
q.DisplayActiveBlocks(ax1,ax2,ax3,yrange)


#Setup some fancy stuff to handle the figure
#There's some bugs that mean we have to do it this way
#to show a matplotlib plot inside a live cgi page 
format = "png"
sio = cStringIO.StringIO()
plt.savefig(sio,format=format,orientation='portrait',papertype='A4',pad_inches=0.0)
#This is how you would save a png to disk
#plt.savefig('queue.png',orientation='portrait',papertype='A4',pad_inches=0.0)
#This is how you would save a postscript to disk
#plt.savefig('plot.eps',orientation='portrait',papertype='A4',pad_inches=0.0)

#Produce a simple html web page interface
print 'Content-type:text/html\r\n\r\n'

print '<html>'
print '<head>'
print '<title>SALT scheduler</title>'
print '</head>'
print '<body>'
print "<table cellpadding=0 border=0 align=left>"
print "<tr>"
print "<td>"

#Setup some fancy stuff to handle the figure
#There's some bugs that mean we have to do it this way
#to show a matplotlib plot inside a live cgi page 
size = 'width="100%" height="100%"'
print "<img border=1 width=600 src=\"data:image/png;base64,%s\" %s/>" % (sio.getvalue().encode("base64").strip(),size)
print "</td>"
print "<td align=left valign=top>"

print "<FORM action=\"/cgi-bin/scheduler.cgi\" method=\"get\">"
print "<b>Date:</b> <input type=\"text\" value=\"%s\" name=\"date\"/>" % cgi_date
print "<br><b>Propcode:</b> <input type=\"text\" value=\"%s\" name=\"propcode\"/>" % cgi_propcode
print "<br><b>PI:</b> <input type=\"text\" value=\"%s\" name=\"piname\"/>" % cgi_piname
print "<br><b>BlockID:</b> <input type=\"text\" value=\"%s\" name=\"blockid\"/>" % cgi_blockid
print "<br>"
print "<input type=\"text\" value=\"%s\" name=\"smin\" size=\"6\"/> <b><= Seeing <=</b> <input type=\"text\" value=\"%s\" name=\"smax\" size=\"6\"/>" % (cgi_smin,cgi_smax)

print "<br><b>MinMoonDist:</b> <input type=\"text\" value=\"%s\" name=\"moondist\" size=\"6\"/>" % cgi_moondist
print "<b>Niter:</b> <input type=\"text\" value=\"%s\" name=\"niter\" size=\"6\"/>" % cgi_niter
print "<br>"
print "<b>Transparency:</b>"
print "<select name=\"tran\"><br>\n";

tsel =""
if(cgi_tran == "Any"):
    tsel = "SELECTED"
print "<option value=\"Any\" %s>Any</option>" % tsel

tsel =""
if(cgi_tran == "Clear"):
    tsel = "SELECTED"
print "<option value=\"Clear\" %s>Clear</option>" % tsel

tsel =""
if(cgi_tran == "NotPhot"):
    tsel = "SELECTED"
print "<option value=\"NotPhot\" %s>Not Photometric</option>" % tsel

tsel =""
if(cgi_tran == "Thin"):
    tsel = "SELECTED"
print "<option value=\"Thin\" %s>Thin Cloud</option>" % tsel

tsel =""
if(cgi_tran == "Thick"):
    tsel = "SELECTED"
print "<option value=\"Thick\" %s>Thick Cloud</option>" % tsel

print "</select>"
print "<br>"

print "<b>Pri Treatment:</b>"
print "<select name=\"ptreat\"><br>\n";

tsel =""
if(cgi_ptreat== "seq"):
    tsel = "SELECTED"
print "<option value=\"seq\" %s>Sequential</option>" % tsel

tsel =""
if(cgi_ptreat == "together"):
    tsel = "SELECTED"
print "<option value=\"together\" %s>Together</option>" % tsel

print "</select>"
print "<br>"

tsel = ""
if(cgi_p0 == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"1\" name=\"p0\" %s/><b>P0</b></label>" % tsel

tsel = ""
if(cgi_p1 == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"p1\" %s/><b>P1</b></label>" % tsel

tsel = ""
if(cgi_p2 == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"p2\" %s/><b>P2</b></label>" % tsel

tsel = ""
if(cgi_p3 == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"p3\" %s/><b>P3</b></label>" % tsel

tsel = ""
if(cgi_p4 == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"p4\" %s/><b>P4</b></label>" % tsel

print "<br>"

tsel = ""
if(cgi_rss == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"1\" name=\"RSS\" %s/><b>RSS</b></label>" % tsel

tsel = ""
if(cgi_scam == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"1\" name=\"SCAM\" %s/><b>SCAM</b></label>" % tsel


tsel = ""
if(cgi_hrs == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"1\" name=\"HRS\" %s/><b>HRS</b></label>" % tsel

tsel = ""
if(cgi_bvit== 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"1\" name=\"BVIT\" %s/><b>BVIT</b></label>" % tsel

print "<br>"
tsel = ""
if(cgi_usealttime == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"usealttime\" %s/><b>Use alternate start time</b></label><input type=\"text\" value=\"%s\" name=\"alttime\" size=\"6\"/>" % (tsel,cgi_alttime)

print "<br>"
tsel = ""
if(cgi_notcmoon == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"notcmoon\" %s/><b>Ignore TimeCritical Moon</b></label>" % tsel

#I've put the (less used) end time here to give it a bit more space from the (more used) start time
#Hopefully that will avoid confusion
print "<br>"
tsel = ""
if(cgi_usealtendtime == 1):
    tsel = "checked"
print "<label><input type=\"checkbox\"  value=\"on\" name=\"usealtendtime\" %s/><b>Use alternate end time</b></label><input type=\"text\" value=\"%s\" name=\"altendtime\" size=\"6\"/>" % (tsel,cgi_altendtime)

print "<br>"
print "<br>"
print "<input type=\"submit\" value=\"submit\">"
print "</FORM>"

#Form ended, now produce the table with summary of the queue

#Get an idea of the quality of the queue
print "Total E = %.1f" % (q.TotalEnergy())
print "\t\t<a href=\"/cheatsheet.txt\" target=\"_\">Cheatsheet</a><br>"
for i in range(0,5):
   colour = 'cyan' #P4 
   if(i == 0):
      colour='black'
   elif (i == 1):
      colour='red'
   elif (i == 2):
      colour='green'
   elif (i == 3):
      colour='blue'
   print "<font color=%s>P%d</font>: %d/%d\t " % (colour,i,q.CountActiveBlocks(i),q.CountInActiveBlocks(i))
print "<br>"

#Print out the schedule in HTML :-)
q.PrintActiveHTML()
print "</td>"
print "</tr>"

print "</table>"
print '</body>'
print '</html>'
