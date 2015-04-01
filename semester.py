#!/usr/bin/python

import MySQLdb

import re,glob,os,sys

from SubBlock import *
from Queue import *

#from cStringIO import *
import numpy as np
import random

#import webbrowser
from matplotlib import pyplot as plt
from matplotlib.patches import Rectangle
#import matplotlib.animation as animation
#from matplotlib.collections import PatchCollection
#from matplotlib.gridspec import gridspec

from matplotlib import dates
import matplotlib.dates as md
from datetime import datetime, timedelta

#http://rhodesmill.org/pyephem/
import ephem
#import re,glob,os,sys


#setup some global things
dtor = np.pi/180.0
fsize=[20,40]
fsize=[8,8]
ymin=-0.3
ymax=5.3
XLAB_ROT=40
XLAB_FSIZE=14
SHOW_FULL=0


def IsOverlapping(Astart,Aend,Bstart,Bend):
   return ( max(Astart,Bstart) <= min(Aend,Bend))


#SETUP SOME DEFAULT PARAMS
cgi_p0 = 1
cgi_p1 = 1
cgi_p2 = 1
cgi_p3 = 1
cgi_p4 = 1 #leaving this out for speed...
cgi_rss = 1
cgi_hrs = 1
cgi_bvit= 0 #bvit not really used
cgi_scam= 1

cgi_smin = 0
cgi_smax = 10
now = datetime.utcnow()+timedelta(hours=2)
if(now.hour >= 0 and now.hour <= 10):
   now = now-timedelta(days=1)
#cgi_date = now.strftime("%Y/%m/%d")
#if(len(sys.argv[1])>0):
#   cgi_date = sys.argv[1]
cgi_alttime = now.strftime("%H:%M")
cgi_usealttime = 0
cgi_altendtime = now.strftime("%H:%M")
cgi_usealtendtime = 0
cgi_tran = "any"
cgi_notcmoon= 0
cgi_propcode = '2014-2'
cgi_piname = ''
cgi_blockid= ''
cgi_moondist = 30.0


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


instrument_list = []
if(cgi_rss):
   instrument_list.append("RSS")
if(cgi_scam):
   instrument_list.append("SCAM")
if(cgi_hrs):
   instrument_list.append("HRS")
if(cgi_bvit):
   instrument_list.append("BVIT")

#Need to set the semester to a default - no observations done...
#Needed a table to keep track of the observed blocks:
#err == index to keep track of whether the block was rejected for some reason (0 = accepted)
#CREATE TABLE BlockTracker (
#Block_Id int(10) unsigned,
#bstart datetime,
#bend datetime,
#err int(10) unsigned,
#Priority int(10) unsigned,
#ObsTime int(10) unsigned
#);
#drop table BlockTracker;

#NightTracker
#CREATE TABLE NightTracker(
#Night datetime,
#Seeing float unsigned,
#dstart datetime,
#dend datetime,
#P0 float unsigned,
#P1 float unsigned,
#P2 float unsigned,
#P3 float unsigned,
#P4 float unsigned,
#Weather float unsigned,
#WeatherSeeing float unsigned,
#WeatherCloud float unsigned,
#WeatherHumidity float unsigned,
#Eng float unsigned,
#Tech float unsigned
#);

#update Block set NDone=0;

sqluser = 'dummy'
sqldb = 'sdb_brent'
sqlhost = 'devsdb'
sqlpasswd='dummy'

#Clear out the old observations
#May need to do reset other quantities here (to check with Encarni)
con = MySQLdb.connect(user=sqluser,db=sqldb,host=sqlhost,passwd=sqlpasswd)
cur = con.cursor()
qtxt = "update Block set NDone=0"
cur.execute(qtxt)
qtxt = "update Block set OnHold=0"
cur.execute(qtxt)
#remove all rows in the BlockTracker table
qtxt = "delete from BlockTracker"
cur.execute(qtxt)
qtxt = "delete from NightTracker"
cur.execute(qtxt)
cur.close()
con.commit()
con.close()

#allow print out of debug info from Queue class and elsewhere...
debug = 1

startdate = "2014/11/01"
enddate = "2014/11/07"
#enddate = "2015/04/30"
#startdate.replace('/','-')
d1 = ephem.localtime(ephem.date(startdate+' 15:00:00'))
Ndays = 7
#d2 = d1+timedelta(days=Ndays)
d2 = ephem.localtime(ephem.date(enddate+' 15:00:00'))

#thorough
cgi_niter = 20
#fast but poor results
#cgi_niter = 5

day = d1
while (day <= d2):
   #pick a random seeing
   cgi_smin = np.random.uniform(1.5,3.0)
   cgi_date = day.strftime("%Y/%m/%d")
   query_date = day.strftime("%Y-%m-%d")
   #a fixed seeing seems a bit too restrictive for now
   cgi_smin = 0

   print "Day: %s Seeing: %.2f" % (cgi_date,cgi_smin)
   seeing_range = [cgi_smin,cgi_smax]
   q = Queue(cgi_date,
             query_date,
             priority_list,
             seeing_range,
             cgi_usealttime,
             cgi_alttime,
             cgi_usealtendtime,
             cgi_altendtime,
             instrument_list,
             cgi_tran,
             cgi_notcmoon,
             cgi_propcode,
             cgi_piname,
             cgi_blockid,
             cgi_moondist,
             sqlhost,
             sqldb,
             debug)


   (dstart,dend) = q.GetTwilightTimes()
   total_duration = q.GetDuration()
   duration_str="%.2f h" % (total_duration)
   x=[dstart-timedelta(minutes=30),dend+timedelta(minutes=30)]
   dt=timedelta(hours=total_duration*1.0/3.0)

   #twilight buffer
   dtwi=timedelta(minutes=30)
   #buffer between plots
   db=timedelta(minutes=5)

   t1=dstart-dtwi
   t2=t1+dtwi+dt
   x1=[t1,t2]
   t1=t2
   t2=t1+dt
   x2=[t1-db,t2+db]
   t1=t2
   t3=dend+dtwi
   x3=[t1,t3]

   print t1, t2, x1, x2

   yrange=[ymin,ymax]
   yoffset=0.05*ymax

   f, (ax1, ax2, ax3) = plt.subplots(nrows=3,ncols=1, sharey=True,subplot_kw=dict(ylim=yrange,yticks=[]),figsize=fsize)
   f.subplots_adjust(hspace=0.3,left=0.05,right=0.95,top=0.925)

   xfmt=md.DateFormatter('%H:%M')

   ax1.xaxis.set_major_formatter(xfmt)
   ax2.xaxis.set_major_formatter(xfmt)
   ax3.xaxis.set_major_formatter(xfmt)
   ax1.set_xlim(x1)
   ax2.set_xlim(x2)
   ax3.set_xlim(x3)
   ax1.xaxis.set_major_locator(md.HourLocator(interval=1))
   ax2.xaxis.set_major_locator(md.HourLocator(interval=1))
   ax3.xaxis.set_major_locator(md.HourLocator(interval=1))

   ax1.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))
   ax2.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))
   ax3.xaxis.set_minor_locator(md.MinuteLocator(byminute=range(0,60,10)))

   ax1.tick_params(which='major',length=7,width=2)
   ax1.tick_params(which='minor',length=5)
   ax2.tick_params(which='major',length=7,width=2)
   ax2.tick_params(which='minor',length=5)
   ax3.tick_params(which='major',length=7,width=2)
   ax3.tick_params(which='minor',length=5)
   #ax1.major_ticks.set_ticksize(10)
   #f.figsize=fsize

   moonstr = ' [%d per cent]' % (q.GetMoonIllum())
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

   #xstart,xend =ax1.get_xlim()
   #ax1.xaxis.set_ticks(np.arange(xstart,xend,timedelta(hours=1.0)))

   ax1.plot([dstart,dstart],yrange,'r--',lw=2.0)
   ax3.plot([dend,dend],yrange,'r--',lw=2.0)

   ti5=dstart-timedelta(minutes=15)
   ti10=dstart-timedelta(minutes=20)
   ti15=dstart-timedelta(minutes=25)
   tf5=dend+timedelta(minutes=5)
   tf10=dend+timedelta(minutes=10)
   tf15=dend+timedelta(minutes=15)

   ax3.plot([tf5,tf5],yrange,'y-',lw=2.0)
   ax3.plot([tf10,tf10],yrange,'y-',lw=2.0)
   ax3.plot([tf15,tf15],yrange,'y-',lw=2.0)
   if(cgi_usealttime == 0):
       ax1.plot([ti5,ti5],yrange,'y-',lw=2.0)
       ax1.plot([ti10,ti10],yrange,'y-',lw=2.0)
       ax1.plot([ti15,ti15],yrange,'y-',lw=2.0)

   moon_alpha = 0.3
   #moon_alpha = 0.5

   (mstart,mend,mduration) = q.GetMoonRange()
   if(mstart != 'NULL' and mend != 'NULL'):
      ax1.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))
      ax2.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))
      ax3.add_patch(Rectangle((mstart,yrange[0]),mduration,yrange[1]*1.5,color='yellow',picker=False,alpha=moon_alpha))

   #The following would do the schedule for each night
   #by optimising the blocks via randomisation and progressively
   #allocating the highest priorities first

#  cgi_niter = 20

#  if(cgi_p0 and cgi_p1):
#     q.RandomiseBlocks(cgi_niter,False,0,1)
#  if(cgi_p1 and cgi_p2):
#     q.RandomiseBlocks(cgi_niter,False,1,2)
#  if(cgi_p2 and cgi_p3):
#     q.RandomiseBlocks(cgi_niter,False,2,3)
#  if(cgi_p3 and cgi_p4):
#     q.RandomiseBlocks(cgi_niter,False,3,4)

   #This does the allocation based on essentially what the SA does at the telescope
   #Results are not as good as RandomiseBlocks approach
   q.MimicSA()

   #display the queue
   q.DisplayActiveBlocks(ax1,ax2,ax3,yrange)
   #Record the queue as observed in the database
   q.MarkActiveAsObserved()
   #Write out stats to each of the tables
   q.UpdateBlockTracker()
   q.UpdateNightTracker()
   #Write out a plot of the queue for the night
   basename = day.strftime("%Y%m%d")
   basename = basename+'.png'
   plt.savefig(basename,orientation='portrait',papertype='A4',pad_inches=0.0)
   #plt.show()

   #Go to the next day
   day = day+timedelta(days=1)

