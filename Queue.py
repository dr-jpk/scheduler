import os,sys, re, glob
import numpy as np
import collections
import bisect
import random
import ephem
import math
import MySQLdb
from SubBlock import *
from matplotlib.patches import Rectangle
from matplotlib import dates
import matplotlib.dates as md
from datetime import datetime, timedelta

class Queue(object):
   def __init__(self,inputdate,priority_list,seeing_range,usealttime,alttime,usealtendtime,altendtime,instrument_list,transparency,notcmoon,propcode,piname,blkid,moondist,host,db,debug):
      self.sqluser = 'brent'
      #self.sqldb = 'sdb_brent'
      self.sqldb = 'sdb'
      self.sqldb = db
      self.sqlhost = host
      self.sqlpasswd='yourpasswordhere'
      #some internal options
      self.debug = debug 
      self.ignorebuffers = 1

      self.smin = seeing_range[0]
      self.smax = seeing_range[1]
      self.priorities = priority_list
      self.usealttime = usealttime
      self.alttime = alttime
      self.usealtendtime = usealtendtime
      self.altendtime = altendtime
      self.pcode = propcode
      self.pi = piname
      self.instruments = instrument_list
      #this is integrated into the SQL query, so no list needed as for instruments
      self.transparency = transparency
      self.ignoretcmoon = notcmoon
      self.blockid = blkid
      self.inputdate = inputdate
      #need to make explicitly sure it's a float - otherwise it won't work
      self.minlunardist = float(moondist)

      self.GetCurrentMosMasks()
      self.GetCurrentRssFilters()
      self.RetrieveNightInfo(inputdate)
      self.DateTimeSetup(inputdate)
      self.LoadBlocks() #could pass on constraints here...?
      self.SHOW_FULL = 1
      
   def RetrieveNightInfo(self,inputdate):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      qtxt = "select MoonPhase_Percent,EveningTwilightEnd,MorningTwilightStart from NightInfo where Date=\'%s\'" % (inputdate.replace('/','-'))
      cur.execute(qtxt)
      results = cur.fetchall()
      for data in results:
         self.moonillum = float(data[0])
         d1 = ephem.date(str(data[1]))
         d2 = ephem.date(str(data[2]))
         self.dstart = d1.datetime()
         self.dend = d2.datetime()
      cur.close()
      con.close()
   def GetCurrentRssFilters(self):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      qtxt = "SELECT Barcode from RssCurrentFilters join RssFilter using (RssFilter_Id) ORDER BY RssFilterSlot"
      cur.execute(qtxt)
      self.loadedrssfilters = { }

      results = cur.fetchall()
      for data in results:
         self.loadedrssfilters[str(data[0])] = str(data[0])
      
      #print self.loadedrssfilters.keys()

      cur.close()
      con.close()
   def GetCurrentMosMasks(self):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      qtxt = "SELECT Barcode from RssCurrentMasks join RssMask using (RssMask_Id) join RssMaskType using (RssMaskType_Id) where Barcode not regexp \'PL\'  ORDER BY RssMaskSlot"
      cur.execute(qtxt)
      self.loadedmosmasks = { }

      results = cur.fetchall()
      for data in results:
         self.loadedmosmasks[str(data[0])] = str(data[0])
      
      #print self.loadedmosmasks.keys()

      cur.close()
      con.close()
   def GetTwilightTimes(self):
      return (self.dstart,self.dend)
   def GetDuration(self):
      #Night duration in hours + decimals for minutes
      return (self.total_duration)
   def LoadBlocks(self):
      #default state - Load all blocks into a list and have none as active 
      #only add blocks that are *visible* tonight [and for time critical blocks, that have at least one time critical window overlapping with self.dstart...self.dend 

      #try and load query data from file if already done....
      #TODO...

      #otherwise, make a new query
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      #for semester simulations...
      #Need to keep NVisits != NDone 
      #Not sure what Current is doing ????
      qtxt = "SELECT DISTINCT b.Block_Id,tc.RaH,tc.RaM,tc.RaS,tc.DecSign,tc.DecD,tc.DecM,tc.DecS,tg.Target_Name,ir.Surname,pc.Proposal_Code,b.Priority,b.PiRanking_Id,tc.EstripS,tc.EstripE,tc.WstripS,tc.WstripE,b.Moon_Id,b.MaxSeeing,b.ObsTime,b.Transparency_Id,b.NVisits,b.NDone,b.WaitDays,b.LastObserved,b.MaxLunarPhase,b.MinLunarAngularDistance from Proposal join ProposalContact as pcon using (Proposal_Id) join Investigator as ir on (pcon.Leader_Id=ir.Investigator_Id) join ProposalCode as pc using (ProposalCode_Id) join Block as b using (Proposal_Id) join Pointing using (Block_Id) join Observation using (Pointing_Id) join Target as tg using (Target_Id) join TargetCoordinates as tc using (TargetCoordinates_Id) WHERE Current=1 and (ProposalStatus_Id=1 or ProposalStatus_Id=4) and OnHold=0 and b.NVisits != b.NDone"
      #for scheduling a night
      #qtxt = "SELECT DISTINCT b.Block_Id,tc.RaH,tc.RaM,tc.RaS,tc.DecSign,tc.DecD,tc.DecM,tc.DecS,tg.Target_Name,ir.Surname,pc.Proposal_Code,b.Priority,b.PiRanking_Id,tc.EstripS,tc.EstripE,tc.WstripS,tc.WstripE,b.Moon_Id,b.MaxSeeing,b.ObsTime,b.Transparency_Id,b.NVisits,b.NDone,b.WaitDays,b.LastObserved,b.MaxLunarPhase,b.MinLunarAngularDistance from Proposal join ProposalContact as pcon using (Proposal_Id) join Investigator as ir on (pcon.Leader_Id=ir.Investigator_Id) join ProposalCode as pc using (ProposalCode_Id) join Block as b using (Proposal_Id) join Pointing using (Block_Id) join Observation using (Pointing_Id) join Target as tg using (Target_Id) join TargetCoordinates as tc using (TargetCoordinates_Id) WHERE Current=1 and ProposalStatus_Id=1 and OnHold=0 and b.NVisits != b.NDone"
      
      NPRI = len(self.priorities)
      if(NPRI >= 1):
         qtxt += " and ("
         for p in range (0,NPRI):
            qtxt += "b.Priority=\'%d\'" % self.priorities[p]
            if(p != NPRI-1 and p+1 < NPRI):
               qtxt += " or "
         qtxt += ")"      

      qtxt += " and b.MaxSeeing >= \'%s\'" % self.smin
      qtxt += " and b.MaxSeeing <= \'%s\'" % self.smax
      #do nothing for 'Any' - no need to filter
      if(self.transparency == "Clear"):
		   qtxt+="and (b.Transparency_Id=\'2\' or b.Transparency_Id=\'1\')"
      if(self.transparency == "NotPhot"):
		   qtxt+="and (b.Transparency_Id=\'5\' or b.Transparency_Id=\'4\' or b.Transparency_Id=\'1\')";
      if(self.transparency == "Thick"):
		   qtxt+="and (b.Transparency_Id=\'4\' or b.Transparency_Id=\'1\')";
      if(self.transparency == "Thin"):
		   qtxt+="and (b.Transparency_Id=\'5\' or b.Transparency_Id=\'1\')";
         
      if(len(self.pcode) >= 1):
         qtxt+=" and (pc.Proposal_Code regexp\'%s\')" % self.pcode
         #qtxt+=" and (pc.Proposal_Code regexp\'%s\' or pc.Proposal_Code regexp\'MLT\')" % self.pcode
      if(len(self.pi) >= 1):
         qtxt+=" and ir.Surname=\'%s\'" % self.pi
      if(len(self.blockid) >= 1):
         qtxt+=" and b.Block_Id=\'%s\'" % self.blockid 

      cur.execute(qtxt)

      #store what each index is in the data array
      #as the query is fixed, we just use numbers here
      #A more elegant solution would be nice and could be revisited at a later date
      BlockID = 0
      RH = 1
      RM = 2
      RS = 3
      DSIGN = 4
      DH = 5
      DM = 6
      DS = 7
      TargetName = 8
      PiName = 9
      PropCode = 10
      Priority = 11
      PiRank = 12
      E1 = 13
      E2 = 14
      W1 = 15
      W2 = 16
      MoonID = 17
      MaxSeeing = 18
      ObsTime = 19
      Transparency = 20
      NVisits = 21
      NDone = 22
      WaitDays = 23
      #To revisit LastObs with pools...
      #this is a tricky param - should be recalculated as most recent of all LastObs entries in blocks for that same target....ugh!
      LastObs = 24 
      MaxLunarPhase = 25
      MinLunarDist = 26

      #setup some parameters
      RA = []
      DEC = []
      
      #our list of available blocks

      self.blist = []
      
      #twilight times based on moon phase
      #5 min buffer for dark blocks
      #10 min buffer for grey
      #15 min buffer for bright
      ti5=self.dstart-timedelta(minutes=15)
      ti10=self.dstart-timedelta(minutes=20)
      ti15=self.dstart-timedelta(minutes=25)
      tf5=self.dend+timedelta(minutes=5)
      tf10=self.dend+timedelta(minutes=10)
      tf15=self.dend+timedelta(minutes=15)
      
      #if we start during the night at a different spot, make sure there's no buffer
      #the end of night twilight can still have a buffer, however, so we leave the tf values alone
      if(self.usealttime):
         ti5=self.dstart
         ti10=self.dstart
         ti15=self.dstart
      if(self.usealtendtime):
         tf5=self.dend
         tf10=self.dend
         tf15=self.dend

      if(self.ignorebuffers):
         ti5 = self.dstart
         ti10 = self.dstart
         ti15 = self.dstart
         tf5 = self.dend
         tf10 = self.dend
         tf15 = self.dend

      
      results = cur.fetchall()
      cur.close()

      #go through each row, representing a block
      #if it is visible/observable tonight, add it to our blist
      for data in results:
         nfields = len(data)
         #calculate RA and DEC in decimal degrees
         ra_deg = int(data[RH])*15.0+int(data[RM])*1.0/60+float(data[RS])*1.0/3600
         dec_deg = int(data[DH])*1.0+int(data[DM])*1.0/60+float(data[DS])*1.0/3600

         if(str(data[DSIGN]) == "-"):
            dec_deg *= -1.0

         #overall visibility windows for the block
         #if there is more than one, we create a separate sub-block
         wstart = []
         wend = []
         
         #print "E2=%s" % data[E2]
         if(str(data[E2]) == "None"):
            wstart.append(float(data[E1]))
            wend.append(float(data[W2]))
         else:
            wstart.append(float(data[E1]))
            wend.append(float(data[E2]))
            wstart.append(float(data[W1]))
            wend.append(float(data[W2]))

         #Now go through each window, and create a sub-block
         for w in range(0,len(wstart)):
            #Not very elegant, but it does the job...
            #TODO: Add NVisits, NDone, WaitDays, LastObs, POOLS
            #(and also add in user filters - min seeing, transparency, etc, as preferences/command line args?)
            
            b = SubBlock(ra_deg,dec_deg,int(data[BlockID]),wstart[w],wend[w],int(data[ObsTime]),str(data[PiName]),str(data[PropCode]),str(data[TargetName]),int(data[Priority]),float(data[MaxSeeing]),int(data[MoonID]),float(data[MaxLunarPhase]),int(data[Transparency]),float(data[MinLunarDist]),int(data[PiRank]),self.ignoretcmoon,int(data[NDone]),int(data[NVisits]))
            #b = SubBlock(ra_deg,dec_deg,int(data[BlockID]),wstart[w],wend[w],int(data[ObsTime]),str(data[PiName]),str(data[PropCode]),str(data[TargetName]),int(data[Priority]),float(data[MaxSeeing]),int(data[MoonID]),float(data[MaxLunarPhase]),int(data[Transparency]),float(data[MinLunarDist]),int(data[PiRank]),self.inputdate)

            #calculate stuff unrelated to time windows...
            b.RetrieveInstrument(con,self.loadedmosmasks,self.loadedrssfilters)
            b.CalcMoonDist(self.moonra,self.moondec)
            (lmin,lmax) = b.GetMoonMinMax()
            ti = ti5
            tf = tf5
            if(lmax >= 85):
               ti = ti15
               tf = tf15
            elif(lmax >= 15):
               ti = ti10
               tf = tf10
            
            self.ti = ti
            self.tf = tf

            #note here we pass dstart (start of twilight) as UT, since we convert track times using UT as the reference
            #that is later converted to SAST using ephem.localtime (see CalcTrackTimes implementation).
            b.CalcTrackTimes(self.lst_min_sast,self.inputdate,con,self.moonillum,self.moonstart,self.moonend,ti,tf)
            #Calculate Energy in the block (dependent on whether time critical or not, as worked out in CalcTrackTimes)
            b.CalcEnergy()
            #b.PrintBPW()

            #ADD POOL INFO
            
            # and b.IsSlitmaskLoaded() 
            #Hrm, HasBPW() should probably not be here...have to work on support for non-sidereal targets?
            if(b.TrackOverlaps(ti,tf) and b.IsTwilightOK() and b.IsMoonOK(self.minlunardist) and b.IsTimeCriticalOK()):# and (b.InstrumentInfo() in self.instruments)):
            #if(b.HasBPW() and b.TrackOverlaps(ti,tf) and b.IsTwilightOK() and b.IsMoonOK(self.minlunardist) and b.IsTimeCriticalOK() and (b.InstrumentInfo() in self.instruments)):
               self.blist.append(b)

      con.close()

               #print "Adding BlockID %d start,end: %s - %s" % (BlockID[i],wstart[w],wend[w])
         #This is where we could work out lists of blockids of blocks that overlap with each block
         #self.CalculateOverlaps()

   def DeactivateBlocks(self):
      for j in filter(lambda x: x.IsActive() == 1,self.blist):
         j.SetActive(0)
#Not currently used
   def CheckOverlaps(self):
      plist = self.blist
      NP = len(plist)
      for i in range(0,NP):
         overlaps = plist[i].TWOverlaps()
         noverlaps = plist[i].NTWOverlaps()
         bid = plist[i].GetID()
         if(noverlaps):
            #print "block: %d noverlaps: %d" % (bid,noverlaps)
            #cool = filter(lambda x: x.GetID() in overlaps,plist)
            for j in range(0,noverlaps):
               overlaps[j].SetActive(1)
            #overlappingblock = filter(lambda x: x.GetID(),overlaps)
            #print overlappingblock[0].GetID()

         #for(j in range(0,noverlaps):
            
#Not currently used
   def CalculateOverlaps(self):
      #work out lists of blockids of blocks that overlap with each block (in the track windows)
      #this forms a starting point for blocks to check whether the chosen pointing time+obstime (which changes) overlaps with other blocks, on the fly, when the optimisation is taking place 
      plist = self.blist
      NP = len(plist)
      for i in range(0,NP):
         #if(plist[i].GetID() == 18506):
         overlaps = []
         (t1,t2) = plist[i].GetStrictWindowTimes()
         for j in range(0,NP):
            if(i != j):
               (u1,u2) = plist[j].GetStrictWindowTimes()
               #if they overlap
               if(max(t1,u1) <= min(t2,u2)):
                  overlaps.append(plist[j])
                  #overlaps.append(plist[j].GetID())
         plist[i].AddTWOverlaps(overlaps)

   def PrintActiveHTML(self):
      #might consider operator.attrgetter for the sorting of larger lists (overlap lists maybe?)
      plist = sorted(filter(lambda x: x.IsActive() == 1,self.blist),key=lambda y: y.GetChosenEnd())
      #plist = sorted(filter(lambda x: x.IsActive(),self.blist),key=lambda y: y.GetChosenStart())
      NP = len(plist)
#     print "<p>General info:"
#     print "Mstart: %s Mend: %s" % (self.moonstart,self.moonend)
#     print "<br>"
#     print "Tstart: %s Tend: %s Minmoon %s" % (self.ti,self.tf,self.minlunardist)
#     print "</p>"
      print "<table align=left border=0 align=left cellpadding=2>"
      print "<tr>"
      print "<td><b>ID</b></td>"
      print "<td><b>Priority</b></td>"
      print "<td><b>Proposal</b></td>"
      print "<td><b>PI</b></td>"
      print "<td><b>Instr</b></td>"
      print "<td><b>Targ</b></td>"
      print "<td><b>Clouds</b></td>"
      print "<td><b>OT</b></td>"
      print "<td><b>Seeing</b></td>"
      print "<td align=center><b>Start</b></td>"
      print "<td align=center><b>End</b></td>"
      print "<td><b>Gap</b></td>"
      print "<td align=center><b>BPW1</b></td>"
      print "<td align=center><b>BPW2</b></td>"
      print "<td align=center><b>Moon</b></td>"
      print "<td align=center><b>MP</b></td>"
      print "</tr>"

      gap_total = 0

      #Add twilight
      print "<tr>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>Twilight</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td align=right>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>%s</td>" % self.dstart.strftime("%H:%M")
      if(NP >= 1):
         if(plist[0].GetChosenStart()-self.dstart >= timedelta(seconds=0)):
            delta = plist[0].GetChosenStart()-self.dstart
            print "<td>+%d</td>" % delta.seconds
            gap_total += delta.seconds
         else:
            delta = self.dstart-plist[0].GetChosenStart()
            print "<td>-%d</td>" % delta.seconds
            gap_total -= delta.seconds
      else: 
         print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "</tr>"

      for p in range(0,NP):
         print "<tr>"
         pri = plist[p].GetPri()
         colour = plist[p].GetColour()
         rank =plist[p].GetPIRank()
        #rank_colour = "blue"
        #if(rank == "Med"):
        #   rank_colour = "green"
        #if(rank == "Hi"):
        #   rank_colour = "red"


         print "<td><a href=\"%s\" target=\"_\">%d</a></td>" % (plist[p].GetWMURL(),plist[p].GetID())
         print "<td><font color=%s>P%d</font> %s</td>" % (colour,plist[p].GetPri(),rank)
         #print "<td><font color=%s>P%d</font> <font color=%s>%s</font></td>" % (colour,plist[p].GetPri(),rank_colour,rank)
         if(plist[p].IsTimeCritical()):
            print "<td><b><font color=magenta>%s</font></b></td>" % plist[p].GetPropCode()
         else:
            print "<td>%s</td>" % plist[p].GetPropCode()
         print "<td>%s</td>" % plist[p].GetPI()

         print "<td>%s</td>" % plist[p].InstrumentInfo()
         print "<td>%s</td>" % plist[p].GetTargetName()
         print "<td>%s</td>" % plist[p].GetTransparency()
         print "<td align=right>%d</td>" % plist[p].GetObsTime()
         print "<td>%.1f</td>" % plist[p].MaxSeeing()

         times = plist[p].GetChosenTimes()
         print "<td align=center>%s</td>" % times[0].strftime("%H:%M")
         print "<td align=center>%s</td>" % times[1].strftime("%H:%M")
         if(p+1 < NP):
            times2 = plist[p+1].GetChosenTimes()
            delta = times2[0]-times[1]
            print "<td><font color=red>%d</font></td>" % delta.seconds
            gap_total += delta.seconds
         if(p == NP-1):
            if(self.dend-times[1] >= timedelta(seconds=0)):
               delta = self.dend-times[1]
               print "<td>+%d</td>" % delta.seconds
               gap_total += delta.seconds
            else:
               delta = times[1]-self.dend
               print "<td>-%d</td>" % delta.seconds
               gap_total -= delta.seconds
         if(plist[p].HasBPW()):
            bpw = plist[p].GetBPW()
            print "<td align=center>%s</td>" % bpw[0].strftime("%H:%M") 
            print "<td align=center>%s</td>" % bpw[1].strftime("%H:%M") 
         else:
            print "<td align=center>-</td>" 
            print "<td align=center>-</td>"

         mtype = plist[p].GetMoonType()
         if(mtype == 'white'):
            print "<td>Any</td>"
         elif(mtype =='black'):
            print "<td bgcolor=black><font color=white>Dark</font></td>"
         elif(mtype =='gray'):
            print "<td bgcolor=grey><font color=white>Grey</font></td>"
         elif(mtype =='yellow'):
            print "<td bgcolor=#ffffae>Bright</td>"
         (lmin,lmax) = plist[p].GetMoonMinMax()
         #if(plist[p].IsTimeWindowActive()):
         #   (time1,time2) = plist[p].GetTimeWindows();
         #   print "<td>%.1f - %.1f; %s; TC=%s to %s</td>" % (lmin,lmax,plist[p].IsTwilightOK(),time1,time2)
         #else:
         print "<td>%.1f - %.1f</td>" % (lmin,lmax)#,plist[p].IsTwilightOK())

         print "</tr>"

      #Add twilight
      print "<tr>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>Twilight</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td align=right>-</td>"
      print "<td align=center>-</td>"
      print "<td align=center>%s</td>" % self.dend.strftime("%H:%M")
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "</tr>"
      
      #Count gaps 
      print "<tr>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>Total Gaps</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td align=right>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>%s <br>(%.2f h)</td>" % (gap_total,gap_total*1.0/3600)
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "<td>-</td>"
      print "</tr>"
            
      print "</table>"

   def IsInsertable(self,block):
      #if each block had a blist that was a subset of self.blist which it potentially overlapped
      #it could be passed here instead of self.blist to speed things up (SUBSTANTIALLY speed things up)
      plist = filter(lambda x: x.IsActive(),self.blist) 
      NP = len(plist)
      #covered by return 1 at end of function
      #if(NP == 0):
      #   return 1
      IsOverlapping = 0
      for i in range(0,NP):
         (t1,t2) = plist[i].GetChosenTimes()
         if(block.IsBlockOverlapping(t1,t2) or block.GetID() == plist[i].GetID()):
         #  print "overlap"
            return 0
      return 1
   def TotalEnergy(self):
      E=0.0
      for j in filter(lambda x: x.IsActive() == 1,self.blist):
         E+=j.Energy()
      return E
#Part of the implementation for weighted interval scheduling method
#may need to be revised 
   def ComputePrevIntervals(self,I):
      start = []
      finish = []
      for i in I:
         start.append(i.GetChosenStart())
         finish.append(i.GetChosenEnd())
      p = []

      for j in xrange(len(I)):
         i = bisect.bisect_right(finish,start[j]) - 1
         p.append(i)
      return p
#Part of the implementation for weighted interval scheduling method
#may need to be revised 
   def ScheduleWeightedIntervalsByPriority(self,pri):
      self.DeactivateBlocks()
      plist = filter(lambda x: x.GetPri() == pri,self.blist)
      I = sorted(plist,key=lambda x: x.GetChosenEnd())
      #plist = sorted(filter(lambda x: x.IsActive(),self.blist),key=lambda y: y.GetChosenEnd())
      #I.sort(lambda x: x.GetChosenEnd())#y: x.GetChosenStart() - y.GetChosenEnd())
      #I.sort(lambda x,y: x.GetChosenStart() - y.GetChosenEnd())
   
      p = self.ComputePrevIntervals(I)

      #OPT...
      OPT = collections.defaultdict(float)
      OPT[-1] = 0
      OPT[0] = 0

      for j in xrange(1,len(I)):
         OPT[j] = max(I[j].Energy() + OPT[p[j]],OPT[j-1])

      O = []
      def compute_solution(j):
         if(j >= 0):
            if(I[j].Energy() + OPT[p[j]] > OPT[j-1]):
               O.append(I[j])
               compute_solution(p[j])
            else:
               compute_solution(j-1)
      compute_solution(len(I)-1)

      for block in O:
         block.SetActive(1)

#Part of the implementation for weighted interval scheduling method
#may need to be revised 
   def ScheduleWeightedIntervals(self):
      self.DeactivateBlocks()
      I = sorted(self.blist,key=lambda x: x.GetChosenEnd())
      #plist = sorted(filter(lambda x: x.IsActive(),self.blist),key=lambda y: y.GetChosenEnd())
      #I.sort(lambda x: x.GetChosenEnd())#y: x.GetChosenStart() - y.GetChosenEnd())
      #I.sort(lambda x,y: x.GetChosenStart() - y.GetChosenEnd())
   
      p = self.ComputePrevIntervals(I)

      #OPT...
      OPT = collections.defaultdict(float)
      OPT[-1] = 0
      OPT[0] = 0

      for j in xrange(1,len(I)):
         OPT[j] = max(I[j].Energy() + OPT[p[j]],OPT[j-1])

      O = []
      def compute_solution(j):
         if(j >= 0):
            if(I[j].Energy() + OPT[p[j]] > OPT[j-1]):
               O.append(I[j])
               compute_solution(p[j])
            else:
               compute_solution(j-1)
      compute_solution(len(I)-1)

      for block in O:
         block.SetActive(1)

   def MimicSA(self):
      #An interesting twist on this function would be to use 
      #sorted(plist, key=lambda x: x.GetObsTime())
      #and always choose the shortest blocks first 
      #(though this might have implications later in the semester with large gaps, making it difficult to fill gaps
      #since all the smaller blocks are finished)

      #N.B. An important thing to recognise with this algorithm is that 
      #the knock-on effects of choosing an early block that may later
      #bump into a high priority block are quite pronounced compared to the
      #RandomiseBlocks approach
      #One can substitute GetPri() here for another function, e.g. GetScore()
      #to use alternative weighting schemes (see also SubBlock.Energy())
      if(self.debug):
         print "MimicSA"
      scores = { }
      plist = self.blist
      NP = len(plist)
      for i in range(0,NP):
         if(not scores.has_key(plist[i].GetPri())):
            scores[plist[i].GetPri()] = plist[i].GetPri()
            if(self.debug):
               print "%d score added" % plist[i].GetPri()
      scorelist = scores.keys()
      scorelist.sort()
      NS = len(scorelist)
      if(self.debug):
         for i in range(0,NS):
            print "score %d = %d" % (i,scorelist[i])


      #loop has to be driven by time - if we find no blocks available in our overlap region,
      #then increment time by 5 min (?) and try again through all priorities
      clock = self.dstart 
      while (clock < self.dend):
         #go through each priority level, starting with the highest one (P0)
         gotone = 0
         for s in range(0,NS):
            if(gotone == 0):
               plist = filter(lambda x: x.GetPri() == scorelist[s] and x.IsActive() == 0,self.blist)
               NP = len(plist)
                  #choose a random index to start with
               if(NP>1):
                  idx = random.randint(0,NP-1)
                  if(plist[idx].OverlapsWindow(clock)):
                     plist[idx].SetChosenStart(clock)
                     if(self.IsInsertable(plist[idx]) and plist[idx].IsActive()==0):
                        plist[idx].SetActive(1)
                        clock = plist[idx].GetChosenEnd()
                        gotone = 1
                        break
                     else:
                        plist[idx].SetDefaultStart()
               else:
                  idx = 0

               #look at next entry in array if the random idx didn't work
               newidx = idx+1
               while(newidx < NP and gotone == 0):
                  if(plist[newidx].OverlapsWindow(clock)):
                     plist[newidx].SetChosenStart(clock)
                     if(self.IsInsertable(plist[newidx]) and plist[newidx].IsActive()==0):
                        plist[newidx].SetActive(1)
                        clock = plist[newidx].GetChosenEnd()
                        gotone = 1
                        break
                     else:
                        plist[newidx].SetDefaultStart()
                  newidx = newidx+1
               #to cover objects earlier in array than the one we chose at random
               newidx = 0
               while(newidx < idx and gotone == 0):
                  if(plist[newidx].OverlapsWindow(clock)):
                     plist[newidx].SetChosenStart(clock)
                     if(self.IsInsertable(plist[newidx]) and plist[newidx].IsActive()==0):
                        plist[newidx].SetActive(1)
                        clock = plist[newidx].GetChosenEnd()
                        gotone = 1
                        break
                     else:
                        plist[newidx].SetDefaultStart()
                  newidx = newidx+1
         #minutes = 1 for finest granularity
         clock = clock+timedelta(minutes=1)

   def RandomiseBlocks(self,iter,inc_all,pfirst,psecond):
      #for j in filter(lambda x: x.IsActive(),self.blist):
      #   j.Randomise()
      if(inc_all):
         plist = self.blist
      else:
         plist = filter(lambda x: x.GetPri() == pfirst or x.GetPri() == psecond,self.blist)
      #plist = self.blist
      #plist = filter(lambda x: x.GetPri() <= 3,self.blist)
      NP = len(plist)

      MaxIters=iter
      for T in range(0,int(MaxIters*0.5)):
         if(self.debug):
            print "T=%.1f E=%.1f" % (T,self.TotalEnergy())
            for pri in range(0,5):
               print "\tP%d: %d/%d\n" % (pri,self.CountActiveBlocks(pri),self.CountInActiveBlocks(pri))
         for i in range(0,MaxIters):
            for idx1 in range(0,NP):
               #randomise this block and another iter times

               #if idx1 is not active, insert it
               #if we can't insert it, randomise its start time in case
               #the next time it's considered, it may be insertable at a different time
               E1=plist[idx1].Energy()
               if(plist[idx1].IsActive() == 0):
                  if(self.IsInsertable(plist[idx1])):
                     plist[idx1].SetActive(1)
                  else:
                     plist[idx1].Randomise()
                     #print "(i) idx1 randomised"
                     plist[idx1].SetActive(0)

               for j in range(0,iter):
                  idx2=random.randint(0,NP-1)
                  if(idx1 != idx2):
                     E2=plist[idx2].Energy()
                     #(i)insert it if it isn't active...
                     #(ii)if that doesn't work, try swapping with idx1 (if E2 > E1)
                     #(iii)otherwise, randomise the start time and leave as inactive
                     #we also need a way to randomise blocks that are active
                     #the swapping takes care of deactivating lower pri blocks
                     if(plist[idx2].IsActive() == 0):
                        if(self.IsInsertable(plist[idx2])):
                           plist[idx2].SetActive(1)
                        elif(plist[idx1].IsActive() == 1 and E2 >= E1):
                           plist[idx1].SetActive(0)
                           if(self.IsInsertable(plist[idx2])):
                              plist[idx2].SetActive(1)
                           else:
                              plist[idx1].SetActive(1)
                              plist[idx2].Randomise()
                              #print "(ii) idx2 randomised"
                        else:
                           plist[idx2].Randomise()
                           #print "(iii) idx2 randomised"
                     #Try and randomise the block if it is already active
                     else: 
                        oldstart = plist[idx2].GetChosenStart()
                        plist[idx2].SetActive(0)
                        plist[idx2].Randomise()
                        #print "(iv) idx2 randomised"
                        if(self.IsInsertable(plist[idx2])):
                           plist[idx2].SetActive(1)
                        else:
                           plist[idx2].SetChosenStart(oldstart)
                           #IsInsertable should strictly always be true here...
                           #if(self.IsInsertable(plist[idx2])):
                           plist[idx2].SetActive(1)
            #Metropolis algorithm
        #print "T=%.2f, E=%.1f" % (T,self.TotalEnergy())
        #for i in range(1,4):
        #   print "\tP%d NActive: %d NInActive: %d" % (i,self.CountActiveBlocks(i),self.CountInActiveBlocks(i))
         #print "E=%.1f" % q.TotalEnergy()
         #T*= dT
   def RandomiseAll(self,Ntries,pri):
#Work in progress...
      plist = filter(lambda x:x.GetPri()==pri,self.blist)
      NP = len(plist)
      #Try Ntries times to randomise the block
      #if unsuccessful, go to the next block
      #(Blocks that are very tight in the track will be difficult to randomise)
      #Ntries = 10
      for j in range(0,Ntries):
        #print "Try %d/%d" % (j,Ntries)
         for i in range(0,NP):
            WasActive=0
            if(plist[i].IsActive() == 1):
               WasActive=1
               plist[i].SetActive(0)
            #for j in range(0,Ntries):
            plist[i].Randomise()
            if(self.IsInsertable(plist[i])):
               plist[i].SetActive(1)
            #if(WasActive and self.IsInsertable(plist[i])):
            #   plist[i].SetActive(1)
            
   def CountActiveObsTimeByInstrument(self,inst):
      plist = filter(lambda x: x.IsActive() == 1 and x.InstrumentInfo() == inst,self.blist)
      NP = len(plist)
      sum = 0
      for i in range(0,NP):
         sum += plist[i].GetObsTime()
      return sum


   def CountActiveObsTime(self):
      plist = filter(lambda x: x.IsActive() == 1,self.blist)
      NP = len(plist)
      sum = 0
      for i in range(0,NP):
         sum += plist[i].GetObsTime()
      return sum

   def CountActiveBlocks(self,priority):
      plist = filter(lambda x: x.IsActive() == 1 and x.GetPri() == priority,self.blist)
      return len(plist)
   def CountInActiveBlocks(self,priority):
      #Doesn't work for P0 - very weird!
      plist2 = filter(lambda x: x.IsActive() == 0 and x.GetPri() == priority,self.blist)
      return len(plist2)

#Active*Blocks* functions are used for debugging (mostly for the plot)
   def ActivateAllBlocks(self,priority):
      plist = filter(lambda x: x.GetPri() == priority,self.blist)
      NP = len(plist)
     #NB = len(self.blist)
      for i in range(0,NP):
         if(self.IsInsertable(plist[i])):
            plist[i].SetActive(1)
   def ActivateAllBlocksUnsafe(self,priority):
      plist = filter(lambda x: x.GetPri() == priority,self.blist)
      NP = len(plist)
     #NB = len(self.blist)
      for i in range(0,NP):
         plist[i].SetActive(1)
   def ActivateSomeBlocks(self,priority):
      plist = filter(lambda x: x.GetPri() == priority,self.blist)
      NP = len(plist)
     #NB = len(self.blist)
      plist[random.randint(0,NP-1)].SetActive(1)
      for i in range(0,NP):
         IsOverlapping = 0
         for j in filter(lambda x: x.IsActive() == 1,plist):
            if(i!=j and plist[i].GetPri() == priority):
               (t1,t2) = j.GetChosenTimes()
               if(plist[i].IsBlockOverlapping(t1,t2)):
                  IsOverlapping = 1
                  break
         if(not IsOverlapping):
            plist[i].SetActive(1)
  #def ListActiveBlocks(self):
  #   for j in filter(lambda x: x.IsActive(),self.blist):
  #      print "%s %s P%s %s" % (j.GetPropCode(),j.GetID(),j.GetPri(),j.GetPI())
   def UpdateNightTracker(self):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()

      dstr = self.dstart.strftime("%Y-%m-%d")# +" 23:59:00"
      sums = [0.0,0.0,0.0,0.0,0.0]
      idle = 0.0
      Weather = 0.0
      WeatherSeeing = 0.0
      WeatherCloud = 0.0
      WeatherHumidity = 0.0
      Eng=0.0
      Tech=0.0
      for pri in range(0,5):
         plist = filter(lambda x: x.IsActive() == 1 and x.GetPri() == pri,self.blist)
         NP = len(plist)
         for j in range(0,NP):
            sums[pri] = sums[pri] + plist[j].GetObsTime()
         sums[pri] = sums[pri]/3600 #get the total in decimal hours
      idle = self.total_duration-(sums[0]+sums[1]+sums[2]+sums[3]+sums[4])

      qtxt = "insert into NightTracker (Night,Seeing,dstart,dend,P0,P1,P2,P3,P4,idle,duration,Weather,WeatherSeeing,WeatherCloud,WeatherHumidity,Eng,Tech) VALUES (\'%s\',%.2f,\'%s\',\'%s\',%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f)" % (dstr,self.smin,self.dstart.strftime("%Y-%m-%d %H:%M:%S"),self.dend.strftime("%Y-%m-%d %H:%M:%S"),sums[0],sums[1],sums[2],sums[3],sums[4],idle,self.total_duration,Weather,WeatherSeeing,WeatherCloud,WeatherHumidity,Eng,Tech)
      print qtxt
      cur.execute(qtxt)
      cur.close()
      con.commit()
      con.close()

   def UpdateBlockTracker(self):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      for j in filter(lambda x: x.IsActive() == 1,self.blist):
         (b1,b2) = j.GetChosenTimes()
         err = 0 #zero = accepted for now...(this is where we can reject blocks etc based weather, eng, etc, specified by different values)
         qtxt = "insert into BlockTracker (Block_Id,bstart,bend,err,Priority,ObsTime) VALUES (%d,\'%s\',\'%s\',%d,%d,%d)" % (j.GetID(),b1.strftime("%Y-%m-%d %H:%M:%S"),b2.strftime("%Y-%m-%d %H:%M:%S"),err,j.GetPri(),j.GetObsTime())
         print qtxt
         cur.execute(qtxt)
      cur.close()
      con.commit()
      con.close()
   def MarkActiveAsObserved(self):
      con = MySQLdb.connect(user=self.sqluser,db=self.sqldb,host=self.sqlhost,passwd=self.sqlpasswd)
      cur = con.cursor()
      for j in filter(lambda x: x.IsActive() == 1,self.blist):
         qtxt = "update Block set NDone=\'%d\' where Block_Id=\'%d\'" % (j.GetNDone()+1,j.GetID())
         print qtxt
         cur.execute(qtxt)
      cur.close()
      con.commit()
      con.close()
   def DisplayActiveBlocks(self,ax1,ax2,ax3,yrange):
      for j in filter(lambda x: x.IsActive() == 1,self.blist):
         for rpatch in j.GetRects(self.SHOW_FULL):
            ax1.add_patch(rpatch)
         for rpatch in j.GetRects(self.SHOW_FULL):
            ax2.add_patch(rpatch)
         for rpatch in j.GetRects(self.SHOW_FULL):
            ax3.add_patch(rpatch)
         #This stuff draws lines on the plot for time window debugging purposes
#        if(j.IsTimeWindowActive()):
#           (d1,d2) = j.GetWindowTimes()
#           ax1.plot([d1,d1],yrange,'b-',lw=2.0)
#           ax1.plot([d2,d2],yrange,'b-',lw=2.0)
#           ax2.plot([d1,d1],yrange,'b-',lw=2.0)
#           ax2.plot([d2,d2],yrange,'b-',lw=2.0)
#           ax3.plot([d1,d1],yrange,'b-',lw=2.0)
#           ax3.plot([d2,d2],yrange,'b-',lw=2.0)
#           
#           (t1,t2) = j.GetTimeWindows()
#           ax1.plot([t1,t1],yrange,'g--',lw=2.0)
#           ax1.plot([t2,t2],yrange,'g--',lw=2.0)
#           ax2.plot([t1,t1],yrange,'g--',lw=2.0)
#           ax2.plot([t2,t2],yrange,'g--',lw=2.0)
#           ax3.plot([t1,t1],yrange,'g--',lw=2.0)
#           ax3.plot([t2,t2],yrange,'g--',lw=2.0)

      
#This is quite important...
   def DateTimeSetup(self,inputdate):
      #When this is called, the twilight times are already loaded by RetrieveNightInfo into self.dstart, self.dend
      #start with setting Sutherland site params
      #Sutherland site parameters
      self.suth=ephem.Observer()
      self.suth.name='Sutherland'
      self.suth.lon='20.808'
      self.suth.lat='-32.379'
      self.suth.elev=1798.0 # *** not used

      self.suth.date=inputdate + ' 23:59:00'

      (self.pmoonset,self.nmoonset,self.pmoonrise,self.nmoonrise,self.moonra,self.moondec)=self.moonstuff(self.suth)

      #put different times into matplotlib format
      self.pmrise=ephem.localtime(ephem.date(self.pmoonrise))
      self.nmrise=ephem.localtime(ephem.date(self.nmoonrise))
      self.pmset=ephem.localtime(ephem.date(self.pmoonset))
      self.nmset=ephem.localtime(ephem.date(self.nmoonset))

      self.suth.date = inputdate + ' 15:00:00'
      #setup an alternate start time
      if(self.usealttime):
         rmod = re.compile('\d+:')
         res = rmod.search(self.alttime)
         hours = res.group()
         hours = int(hours.replace(':',''))
         
         rmod = re.compile(':\d+')
         res = rmod.search(self.alttime)
         mins = res.group()
         mins = int(mins.replace(':',''))

         if(hours < 10):
            hours += 24
         #how many hours in self.dstart? 
         new_time = hours*1.0 + mins*1.0/60
         old_time = self.dstart.hour*1.0+self.dstart.minute*1.0/60+self.dstart.second*1.0/3600
         delta_time = new_time-old_time
         if(delta_time > 0):
            self.dstart = ephem.localtime(ephem.date(self.dstart + timedelta(hours=delta_time)-timedelta(hours=2)))

      #setup an alternate end time
      if(self.usealtendtime):
         rmod = re.compile('\d+:')
         res = rmod.search(self.altendtime)
         hours = res.group()
         hours = int(hours.replace(':',''))
         
         rmod = re.compile(':\d+')
         res = rmod.search(self.altendtime)
         mins = res.group()
         mins = int(mins.replace(':',''))

         if(hours > 10):
            hours -= 24
         #how many hours in self.dend? 
         new_time = hours*1.0 + mins*1.0/60
         old_time = self.dend.hour*1.0+self.dend.minute*1.0/60+self.dend.second*1.0/3600
         delta_time = new_time-old_time
         if(new_time < old_time):
            self.dend = ephem.localtime(ephem.date(self.dend + timedelta(hours=delta_time)-timedelta(hours=2)))

      #Get the LST time
      lst=self.suth.sidereal_time()
      #print "LST=%s" % str(lst)
      lst_t=re.split(':',str(lst))
      #print type(lst_t[0])
      #print "%.0f %.0f %.2f" % (float(lst_t[0]),float(lst_t[1]),float(lst_t[2]))
      #calculate decimal value of LST at evening twilight
      self.lst_evening_twilight=float(lst_t[0])*1.0+float(lst_t[1])*1.0/60+float(lst_t[2])*1.0/3600

      #The additional -2.0 hours here is to put it into SAST...
      self.lst_min_sast = self.lst_evening_twilight - 15.00 - 2.0

      #work out the night duration
      #(not really used right now)
      self.duration=(self.dend-self.dstart)
      ds=self.duration.seconds
      self.duration_hours=ds/3600
      self.duration_minutes= (ds % 3600)/60
      #self.duration_str="%d h %d m" % (self.duration_hours,self.duration_minutes)
      self.total_duration=self.duration_hours + self.duration_minutes*1.0/60

      #PMR = Previous Moon Rise
      #PMS = Previous Moon Set
      #NMR = Next Moon Rise
      #NMS = Next Moon Set
      self.PMR=md.date2num(self.pmrise)
      self.PMS=md.date2num(self.pmset)
      self.NMR=md.date2num(self.nmrise)
      self.NMS=md.date2num(self.nmset)
      
      #work out what times to use for moon range for the night
      #Complex....
      self.CalcMoonRange()

   def GetMoonIllum(self):
      return float(self.moonillum)
   def GetMoonRange(self):
      return (self.moonstart,self.moonend,self.mduration)
   def CalcMoonRange(self):
      #This function is very messy!   
      #buffer either side of twilight to consider for moon range...
      dtwi=timedelta(minutes=30)
      self.moonstart='NULL'
      self.moonend='NULL'
      self.mduration = 0

      if(self.pmrise >= self.dstart-dtwi and self.pmrise <= self.dend+dtwi):
         self.mstart=self.PMR
         if(self.pmset-self.pmrise >= timedelta(seconds=0)):
            self.mduration = self.PMS-self.PMR
            self.moonstart=self.pmrise
            self.moonend=self.pmset
         elif (self.nmset-self.pmrise > timedelta(seconds=0)):
            self.mduration = self.NMS-self.PMR
            self.moonstart=self.pmrise
            self.moonend=self.nmset
      elif(self.pmset >= self.dstart-dtwi and self.pmset <= self.dend+dtwi):
         self.mstart=self.PMR
         if(self.pmset-self.pmrise >= timedelta(seconds=0)):
            self.mduration = self.PMS-self.PMR
            self.moonstart=self.pmrise
            self.moonend=self.pmset

      elif(self.nmrise >= self.dstart-dtwi and self.nmrise <= self.dend+dtwi):
         self.mstart=self.NMR
         if(self.pmset-self.nmrise >= timedelta(seconds=0)):
            self.mduration =  self.PMS-self.NMR
            self.moonstart=self.nmrise
            self.moonend=self.pmset
         elif (self.nmset - self.nmrise >= timedelta(seconds=0)):
            self.mduration = self.NMS-self.NMR
            self.moonstart=self.nmrise
            self.moonend=self.nmset
      elif(self.nmset >= self.dstart-dtwi and self.nmset <= self.dend+dtwi):
         self.mstart=0
         if(self.nmset-self.pmrise >= timedelta(seconds=0)):
            self.mduration =  self.NMS-self.PMR
            self.mstart=self.pmrise
            self.moonstart=self.pmrise
            self.moonend=self.nmset
         elif (self.nmset - self.nmrise >= timedelta(seconds=0)):
            self.mduration = self.NMS-self.NMR
            self.mstart=self.nmrise
            self.moonstart=self.nmrise
            self.moonend=self.nmset

      elif(self.nmset - self.pmrise >= timedelta(seconds=0) and self.pmset - self.pmrise < timedelta(seconds=0)):
         self.mduration =  self.NMS-self.PMR
         self.mstart=self.pmrise
         self.moonstart=self.pmrise
         self.moonend=self.nmset

   #calc moon rise/set times, illum frac, and position
   #could in theory be replaced with times from NightInfo, but I already wrote all this before thinking about that!
   def moonstuff(self,site):
      site.horizon='0.'
      moon=ephem.Moon(site)
      pmoonset=ephem.date(site.previous_setting(moon))
      nmoonset=ephem.date(site.next_setting(moon))
      nmoonrise=ephem.date(site.next_rising(moon))
      pmoonrise=ephem.date(site.previous_rising(moon))
      #replaced moon illum by database query
      #moonillum=moon.moon_phase
      moonra=moon.g_ra
      moondec=moon.g_dec
      return (pmoonset,nmoonset,pmoonrise,nmoonrise,moonra,moondec)





