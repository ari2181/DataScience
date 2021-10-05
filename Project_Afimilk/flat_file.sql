USE DS_PROJECT
GO

-- Convert date column from text into date format
drop table if exists cow_data

SELECT distinct [FarmCode]
      ,[DateMonth]
      ,CASE WHEN ([Cur_Date] != '0') THEN (convert(date, [Cur_Date], 103)) END as [Cur_Date]
      ,[DIM]
      ,[CowID]
	  ,[FarmCowID]
	  ,CASE WHEN ([BirthDate] <> '0') THEN (convert(date, [BirthDate], 103)) END as [BirthDate]
      ,CASE WHEN ([CalvingDate] <> '0') THEN (convert(date, [CalvingDate], 103)) END as [CalvingDate]
      ,CASE WHEN ([CurrentConceivedDate] <> '0') THEN (convert(date, [CurrentConceivedDate], 103)) END as [CurrentConceivedDate]
      ,CASE WHEN ([CullingDate] <> '0') THEN (convert(date, [CullingDate], 103)) END as [CullingDate]
      ,[Breed]
      ,[Sys_Status]
      ,[Gynecology_Status]
      ,[Herd_ID]
      ,[Group_ID]
      ,[Extended_Status]
      ,[Heifer_Extend_Status]
      ,[LactationNumber]
      ,[IsPreviousLactation]
      ,[CurrentDryDays]
      ,[Is_Milked]
      ,[Is_Ready_to_breed]
      ,[Fertility_Num]
      ,[DailyYield_KG]
      ,[TenDaysAvgYield]
      ,[AccumulateYieldFromDay4]
      ,[DailyFat_P]
      ,[TenDaysAvgFat_P]
      ,[DailyFat_KG]
      ,[TenDaysAvgFat_KG]
      ,[AccumulateFatFromDay4_KG]
      ,[DailyProtein_P]
      ,[TenDaysAvgProtein_p]
      ,[DailyProtein_KG]
      ,[TenDaysAvgProtein_KG]
      ,[AccumulateProteinFromDay4_KG]
      ,[ECM_KG]
      ,[TenDaysAvgECM_KG]
      ,[AccumulateECMFromDay4_KG]
      ,[DailyConductivity]
      ,[TenDaysAvgConductivity]
      ,[DailyProdRate]
      ,[DailyMilkingTime]
      ,[DailyActivity]
      ,[DailyHeatIndicator]
      ,[DailyTemperature]
      ,[DailyRestRatio]
      ,[DailyRestRestlessness]
      ,[DailyRestPerBout]
      ,[DailyRestTime]
      ,[DailyRestBout]
      ,[DailyFeedMode]
      ,[DailyFeedAllocationTime]
      ,[DailyBCS]
      ,[BCSAtCalving]
      ,[BCSAt4060]
      ,[Bm15]
      ,[Bm610]
      ,[Bm1115]
      ,[Bm1620]
      ,[WeightCalv]
      ,[DailyWeight]
      ,[DailyHeight]
      ,[CurrentRP]
      ,[CurrentMET]
      ,[CurrentKET]
      ,[CurrentMF]
      ,[CurrentPRO]
      ,[CurrentLDA]
      ,[CurrentMAST]
      ,[CurrentEdma]
      ,[CurrentLAME]
      ,[Parity123]
      --,[DaysRemainingTo305] /*Data isn't consist*/
      ,[CS_CalvingMonth]
      --,[DRCS] /*Data isn't consist*/
      ,[Age]
	  --,[DRAI] /*Data isn't consist*/
      --,[DRDR] /*Data isn't consist*/
      ,[Twin]
      ,[Still]
      --,[DRDIM] /*Data isn't consist*/
      --,[DRDIMDIM] /*Data isn't consist*/
      ,[DRDP]
      ,[DRDPDP]
      ,[PrevDryDays]
      ,[DDDD]
      ,[Term]
      ,[TermTerm]
      ,[PPUD]
      ,[DailyFPR_P]
      ,[TenDaysAvgFPR_P]
      --,[LRMDR] /*Data isn't consist*/
      --,[LRMLRMDR] /*Data isn't consist*/
      --,[LRFDR] /*Data isn't consist*/
      --,[LRFLRFDR] /*Data isn't consist*/
      --,[LRPDR] /*Data isn't consist*/
      --,[LRPLRPDR] /*Data isn't consist*/
      --,[LRECMDR] /*Data isn't consist*/
      --,[LRECMLRECMDR] /*Data isn't consist*/
      --,[ConcDays] /*Data isn't consist*/
      ,[DP]
  into cow_data
  FROM [DS_PROJECT].[dbo].[cow_data_raw]
  order by  CowID

GO

--------------------------------------------------------------------------------------
-- add 7 days LEADs to the data

drop table if exists cow_data_t7

select	FarmCode
		,DateMonth
		,DIM
		,CowID
		,FarmCowID
		,BirthDate
		,CalvingDate
		,Cur_Date
		,CurrentConceivedDate
		,Breed
		,Sys_Status
		,Gynecology_Status
		,Herd_ID
		,Extended_Status
		,Heifer_Extend_Status
		,LactationNumber
		,IsPreviousLactation
		,CurrentDryDays
		,Is_Ready_to_breed
		,Fertility_Num
		,WeightCalv
		,CurrentRP
		,CurrentMET
		,CurrentKET
		,CurrentMF
		,CurrentPRO
		,CurrentLDA
		,CurrentMAST
		,CurrentEdma
		,CurrentLAME
		,Parity123
		,Age
		,DATEDIFF(MONTH, BirthDate, CalvingDate) as Age_calc
		,Twin
		,Still
		,PrevDryDays
		,Term
		,PPUD
		,Group_ID
		,LEAD(Group_ID, 1) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t1
		,LEAD(Group_ID, 2) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t2
		,LEAD(Group_ID, 3) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t3
		,LEAD(Group_ID, 4) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t4
		,LEAD(Group_ID, 5) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t5
		,LEAD(Group_ID, 6) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t6
		,LEAD(Group_ID, 7) OVER(partition by FarmCowID order by Cur_Date) as Group_ID_t7
		,Is_Milked
		,LEAD(Is_Milked, 1) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t1
		,LEAD(Is_Milked, 2) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t2
		,LEAD(Is_Milked, 3) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t3
		,LEAD(Is_Milked, 4) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t4
		,LEAD(Is_Milked, 5) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t5
		,LEAD(Is_Milked, 6) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t6
		,LEAD(Is_Milked, 7) OVER(partition by FarmCowID order by Cur_Date) as Is_Milked_t7
		, DailyYield_KG 
		, LEAD(DailyYield_KG, 1) OVER(partition by	FarmCowID order by Cur_Date) - DailyYield_KG as DailyYield_KG_t1
		, LEAD(DailyYield_KG, 2) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 1) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t2
		, LEAD(DailyYield_KG, 3) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 2) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t3
		, LEAD(DailyYield_KG, 4) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 3) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t4
		, LEAD(DailyYield_KG, 5) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 4) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t5
		, LEAD(DailyYield_KG, 6) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 5) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t6
		, LEAD(DailyYield_KG, 7) OVER(partition by	FarmCowID order by Cur_Date) - LEAD(DailyYield_KG, 6) OVER(partition by	FarmCowID order by Cur_Date) as DailyYield_KG_t7
		--, TenDaysAvgYield
		--, LEAD(TenDaysAvgYield, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgYield as TenDaysAvgYield_t1
		--, LEAD(TenDaysAvgYield, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t2
		--, LEAD(TenDaysAvgYield, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t3
		--, LEAD(TenDaysAvgYield, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t4
		--, LEAD(TenDaysAvgYield, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t5
		--, LEAD(TenDaysAvgYield, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t6
		--, LEAD(TenDaysAvgYield, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgYield, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t7
		, LEAD(TenDaysAvgYield, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgYield_t7
		--, AccumulateYieldFromDay4
		--, LEAD(AccumulateYieldFromDay4, 1) OVER(partition by FarmCowID order by Cur_Date) - AccumulateYieldFromDay4 as AccumulateYieldFromDay4_t1
		--, LEAD(AccumulateYieldFromDay4, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 1) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t2
		--, LEAD(AccumulateYieldFromDay4, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 2) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t3
		, LEAD(AccumulateYieldFromDay4, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 3) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t4
		, LEAD(AccumulateYieldFromDay4, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 4) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t5
		, LEAD(AccumulateYieldFromDay4, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 5) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t6
		, LEAD(AccumulateYieldFromDay4, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateYieldFromDay4, 6) OVER(partition by FarmCowID order by Cur_Date) as AccumulateYieldFromDay4_t7
		, DailyFat_P
		, LEAD(DailyFat_P, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyFat_P as DailyFat_P_t1
		, LEAD(DailyFat_P, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t2
		, LEAD(DailyFat_P, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t3
		, LEAD(DailyFat_P, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t4
		, LEAD(DailyFat_P, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t5
		, LEAD(DailyFat_P, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t6
		, LEAD(DailyFat_P, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_P, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_P_t7
		--, TenDaysAvgFat_P
		--, LEAD(TenDaysAvgFat_P, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgFat_P as TenDaysAvgFat_P_t1
		--, LEAD(TenDaysAvgFat_P, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t2
		--, LEAD(TenDaysAvgFat_P, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t3
		--, LEAD(TenDaysAvgFat_P, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t4
		--, LEAD(TenDaysAvgFat_P, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t5
		--, LEAD(TenDaysAvgFat_P, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t6
		--, LEAD(TenDaysAvgFat_P, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_P, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t7
		, LEAD(TenDaysAvgFat_P, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_P_t7
		, DailyFat_KG
		, LEAD(DailyFat_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyFat_KG as DailyFat_KG_t1
		, LEAD(DailyFat_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t2
		, LEAD(DailyFat_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t3
		, LEAD(DailyFat_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t4
		, LEAD(DailyFat_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t5
		, LEAD(DailyFat_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t6
		, LEAD(DailyFat_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFat_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyFat_KG_t7
		--, TenDaysAvgFat_KG
		--, LEAD(TenDaysAvgFat_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgFat_KG as TenDaysAvgFat_KG_t1
		--, LEAD(TenDaysAvgFat_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t2
		--, LEAD(TenDaysAvgFat_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t3
		--, LEAD(TenDaysAvgFat_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t4
		--, LEAD(TenDaysAvgFat_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t5
		--, LEAD(TenDaysAvgFat_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t6
		--, LEAD(TenDaysAvgFat_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFat_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t7
		, LEAD(TenDaysAvgFat_KG, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFat_KG_t7
		--, AccumulateFatFromDay4_KG
		--, LEAD(AccumulateFatFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - AccumulateFatFromDay4_KG as AccumulateFatFromDay4_KG_t1
		--, LEAD(AccumulateFatFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t2
		--, LEAD(AccumulateFatFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t3
		, LEAD(AccumulateFatFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t4
		, LEAD(AccumulateFatFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t5
		, LEAD(AccumulateFatFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t6
		, LEAD(AccumulateFatFromDay4_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateFatFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as AccumulateFatFromDay4_KG_t7
		, DailyProtein_P
		, LEAD(DailyProtein_P, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyProtein_P as DailyProtein_P_t1
		, LEAD(DailyProtein_P, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t2
		, LEAD(DailyProtein_P, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t3
		, LEAD(DailyProtein_P, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t4
		, LEAD(DailyProtein_P, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t5
		, LEAD(DailyProtein_P, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t6
		, LEAD(DailyProtein_P, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_P, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_P_t7
		--, TenDaysAvgProtein_p
		--, LEAD(TenDaysAvgProtein_p, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgProtein_p as TenDaysAvgProtein_p_t1
		--, LEAD(TenDaysAvgProtein_p, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t2
		--, LEAD(TenDaysAvgProtein_p, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t3
		--, LEAD(TenDaysAvgProtein_p, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t4
		--, LEAD(TenDaysAvgProtein_p, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t5
		--, LEAD(TenDaysAvgProtein_p, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t6
		--, LEAD(TenDaysAvgProtein_p, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_p, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t7
		, LEAD(TenDaysAvgProtein_p, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_p_t7
		, DailyProtein_KG
		, LEAD(DailyProtein_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyProtein_KG as DailyProtein_KG_t1
		, LEAD(DailyProtein_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t2
		, LEAD(DailyProtein_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t3
		, LEAD(DailyProtein_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t4
		, LEAD(DailyProtein_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t5
		, LEAD(DailyProtein_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t6
		, LEAD(DailyProtein_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProtein_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyProtein_KG_t7
		--, TenDaysAvgProtein_KG
		--, LEAD(TenDaysAvgProtein_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgProtein_KG as TenDaysAvgProtein_KG_t1
		--, LEAD(TenDaysAvgProtein_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t2
		--, LEAD(TenDaysAvgProtein_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t3
		--, LEAD(TenDaysAvgProtein_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t4
		--, LEAD(TenDaysAvgProtein_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t5
		--, LEAD(TenDaysAvgProtein_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t6
		--, LEAD(TenDaysAvgProtein_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgProtein_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t7
		, LEAD(TenDaysAvgProtein_KG, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgProtein_KG_t7
		--, AccumulateProteinFromDay4_KG
		--, LEAD(AccumulateProteinFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - AccumulateProteinFromDay4_KG as AccumulateProteinFromDay4_KG_t1
		--, LEAD(AccumulateProteinFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t2
		--, LEAD(AccumulateProteinFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t3
		--, LEAD(AccumulateProteinFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t4
		, LEAD(AccumulateProteinFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t5
		, LEAD(AccumulateProteinFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t6
		, LEAD(AccumulateProteinFromDay4_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateProteinFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as AccumulateProteinFromDay4_KG_t7
		, ECM_KG
		, LEAD(ECM_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - ECM_KG as ECM_KG_t1
		, LEAD(ECM_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t2
		, LEAD(ECM_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t3
		, LEAD(ECM_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t4
		, LEAD(ECM_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t5
		, LEAD(ECM_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t6
		, LEAD(ECM_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(ECM_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as ECM_KG_t7
		, TenDaysAvgECM_KG
		, LEAD(TenDaysAvgECM_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgECM_KG as TenDaysAvgECM_KG_t1
		, LEAD(TenDaysAvgECM_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t2
		, LEAD(TenDaysAvgECM_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t3
		, LEAD(TenDaysAvgECM_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t4
		, LEAD(TenDaysAvgECM_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t5
		, LEAD(TenDaysAvgECM_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t6
		, LEAD(TenDaysAvgECM_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgECM_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgECM_KG_t7
		--, AccumulateECMFromDay4_KG
		--, LEAD(AccumulateECMFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) - AccumulateECMFromDay4_KG as AccumulateECMFromDay4_KG_t1
		--, LEAD(AccumulateECMFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 1) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t2
		--, LEAD(AccumulateECMFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 2) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t3
		, LEAD(AccumulateECMFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 3) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t4
		, LEAD(AccumulateECMFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 4) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t5
		, LEAD(AccumulateECMFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 5) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t6
		, LEAD(AccumulateECMFromDay4_KG, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(AccumulateECMFromDay4_KG, 6) OVER(partition by FarmCowID order by Cur_Date) as AccumulateECMFromDay4_KG_t7
		, DailyConductivity
		, LEAD(DailyConductivity, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyConductivity as DailyConductivity_t1
		, LEAD(DailyConductivity, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t2
		, LEAD(DailyConductivity, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t3
		, LEAD(DailyConductivity, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t4
		, LEAD(DailyConductivity, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t5
		, LEAD(DailyConductivity, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t6
		, LEAD(DailyConductivity, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyConductivity, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyConductivity_t7
		--, TenDaysAvgConductivity
		--, LEAD(TenDaysAvgConductivity, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgConductivity as TenDaysAvgConductivity_t1
		--, LEAD(TenDaysAvgConductivity, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t2
		--, LEAD(TenDaysAvgConductivity, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t3
		--, LEAD(TenDaysAvgConductivity, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t4
		--, LEAD(TenDaysAvgConductivity, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t5
		--, LEAD(TenDaysAvgConductivity, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t6
		--, LEAD(TenDaysAvgConductivity, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgConductivity, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t7
		, LEAD(TenDaysAvgConductivity, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgConductivity_t7
		, DailyProdRate
		, LEAD(DailyProdRate, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyProdRate as DailyProdRate_t1
		, LEAD(DailyProdRate, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t2
		, LEAD(DailyProdRate, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t3
		, LEAD(DailyProdRate, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t4
		, LEAD(DailyProdRate, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t5
		, LEAD(DailyProdRate, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t6
		, LEAD(DailyProdRate, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyProdRate, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyProdRate_t7
		, DailyMilkingTime
		, LEAD(DailyMilkingTime, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyMilkingTime as DailyMilkingTime_t1
		, LEAD(DailyMilkingTime, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t2
		, LEAD(DailyMilkingTime, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t3
		, LEAD(DailyMilkingTime, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t4
		, LEAD(DailyMilkingTime, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t5
		, LEAD(DailyMilkingTime, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t6
		, LEAD(DailyMilkingTime, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyMilkingTime, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyMilkingTime_t7
		, DailyActivity
		, LEAD(DailyActivity, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyActivity as DailyActivity_t1
		, LEAD(DailyActivity, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t2
		, LEAD(DailyActivity, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t3
		, LEAD(DailyActivity, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t4
		, LEAD(DailyActivity, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t5
		, LEAD(DailyActivity, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t6
		, LEAD(DailyActivity, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyActivity, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyActivity_t7
		, DailyHeatIndicator
		, LEAD(DailyHeatIndicator, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyHeatIndicator as DailyHeatIndicator_t1
		, LEAD(DailyHeatIndicator, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t2
		, LEAD(DailyHeatIndicator, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t3
		, LEAD(DailyHeatIndicator, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t4
		, LEAD(DailyHeatIndicator, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t5
		, LEAD(DailyHeatIndicator, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t6
		, LEAD(DailyHeatIndicator, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyHeatIndicator, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyHeatIndicator_t7
		, DailyRestRatio
		, LEAD(DailyRestRatio, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyRestRatio as DailyRestRatio_t1
		, LEAD(DailyRestRatio, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t2
		, LEAD(DailyRestRatio, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t3
		, LEAD(DailyRestRatio, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t4
		, LEAD(DailyRestRatio, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t5
		, LEAD(DailyRestRatio, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t6
		, LEAD(DailyRestRatio, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRatio, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRatio_t7
		, DailyRestRestlessness
		, LEAD(DailyRestRestlessness, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyRestRestlessness as DailyRestRestlessness_t1
		, LEAD(DailyRestRestlessness, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t2
		, LEAD(DailyRestRestlessness, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t3
		, LEAD(DailyRestRestlessness, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t4
		, LEAD(DailyRestRestlessness, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t5
		, LEAD(DailyRestRestlessness, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t6
		, LEAD(DailyRestRestlessness, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestRestlessness, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyRestRestlessness_t7
		, DailyRestPerBout
		, LEAD(DailyRestPerBout, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyRestPerBout as DailyRestPerBout_t1
		, LEAD(DailyRestPerBout, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t2
		, LEAD(DailyRestPerBout, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t3
		, LEAD(DailyRestPerBout, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t4
		, LEAD(DailyRestPerBout, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t5
		, LEAD(DailyRestPerBout, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t6
		, LEAD(DailyRestPerBout, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestPerBout, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyRestPerBout_t7
		, DailyRestTime
		, LEAD(DailyRestTime, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyRestTime as DailyRestTime_t1
		, LEAD(DailyRestTime, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t2
		, LEAD(DailyRestTime, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t3
		, LEAD(DailyRestTime, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t4
		, LEAD(DailyRestTime, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t5
		, LEAD(DailyRestTime, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t6
		, LEAD(DailyRestTime, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestTime, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyRestTime_t7
		, DailyRestBout
		, LEAD(DailyRestBout, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyRestBout as DailyRestBout_t1
		, LEAD(DailyRestBout, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t2
		, LEAD(DailyRestBout, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t3
		, LEAD(DailyRestBout, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t4
		, LEAD(DailyRestBout, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t5
		, LEAD(DailyRestBout, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t6
		, LEAD(DailyRestBout, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyRestBout, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyRestBout_t7
		, DailyFPR_P
		, LEAD(DailyFPR_P, 1) OVER(partition by FarmCowID order by Cur_Date) - DailyFPR_P as DailyFPR_P_t1
		, LEAD(DailyFPR_P, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 1) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t2
		, LEAD(DailyFPR_P, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 2) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t3
		, LEAD(DailyFPR_P, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 3) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t4
		, LEAD(DailyFPR_P, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 4) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t5
		, LEAD(DailyFPR_P, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 5) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t6
		, LEAD(DailyFPR_P, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(DailyFPR_P, 6) OVER(partition by FarmCowID order by Cur_Date) as DailyFPR_P_t7
		--, TenDaysAvgFPR_P
		--, LEAD(TenDaysAvgFPR_P, 1) OVER(partition by FarmCowID order by Cur_Date) - TenDaysAvgFPR_P as TenDaysAvgFPR_P_t1
		--, LEAD(TenDaysAvgFPR_P, 2) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 1) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t2
		--, LEAD(TenDaysAvgFPR_P, 3) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 2) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t3
		--, LEAD(TenDaysAvgFPR_P, 4) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 3) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t4
		--, LEAD(TenDaysAvgFPR_P, 5) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 4) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t5
		--, LEAD(TenDaysAvgFPR_P, 6) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 5) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t6
		--, LEAD(TenDaysAvgFPR_P, 7) OVER(partition by FarmCowID order by Cur_Date) - LEAD(TenDaysAvgFPR_P, 6) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t7
		, LEAD(TenDaysAvgFPR_P, 7) OVER(partition by FarmCowID order by Cur_Date) as TenDaysAvgFPR_P_t7

into cow_data_t7	
from cow_data
order by FarmCowID, CalvingDate, Cur_Date

GO

-------------------------------------------------------------------------------------------


-- Table of cows and its calving dates
drop table if exists cow_calving_date

--select distinct FarmCowID, CalvingDate
--into cow_calving_date
--from cow_data
--where CalvingDate is not NULL
--and CalvingDate >= (select min(Cur_Date) from cow_data)

select distinct FarmCowID, CalvingDate
into cow_calving_date
from cow_data
where CalvingDate is not NULL
and Cur_Date = DATEADD(day, 1, CalvingDate)
order by FarmCowID,CalvingDate

--find cows which had disease in the first month - calc LAST for FF
drop table if exists cur_disease_t7_t30
select distinct ccd.FarmCowID
		,cd.CalvingDate
		,CASE WHEN (sum(cd.CurrentMET) > 0) THEN (1) ELSE (0) END as CurMet_t1_t30
		,CASE WHEN (sum(cd.CurrentKET) > 0) THEN (1) ELSE (0) END as CurKET_t1_t30
		,CASE WHEN (sum(cd.CurrentMF) > 0) THEN (1) ELSE (0) END as CurMF_t1_t30
		,CASE WHEN (sum(cd.CurrentPRO) > 0) THEN (1) ELSE (0) END as CurPRO_t1_t30
		,CASE WHEN (sum(cd.CurrentLDA) > 0) THEN (1) ELSE (0) END as CurLDA_t1_t30
		,CASE WHEN (sum(cd.CurrentMAST) > 0) THEN (1) ELSE (0) END as CurMAST_t1_t30
		,CASE WHEN (sum(cd.CurrentEdma) > 0) THEN (1) ELSE (0) END as CurEdma_t1_t30
		,CASE WHEN (sum(cd.CurrentLAME) > 0) THEN (1) ELSE (0) END as CurLAME_t1_t30
into cur_disease_t7_t30
from cow_calving_date ccd
left outer join cow_data cd
	on ccd.FarmCowID = cd.FarmCowID	and ccd.CalvingDate = DATEADD(day, -1, cd.Cur_Date)
where cd.Cur_Date between ccd.CalvingDate and DATEADD(DAY, 30, ccd.CalvingDate )
group by ccd.FarmCowID, cd.CalvingDate
ORDER BY ccd.FarmCowID, cd.CalvingDate

--find cows which had disease in the first 7 days - enter to FF
drop table if exists cur_disease_t1_t7
select distinct ccd.FarmCowID
		,cd.CalvingDate
		--,CASE WHEN (sum(cd.CurrentMET) > 0) THEN (1) ELSE (0) END as CurMet_t1_t7
		,CASE WHEN (sum(cd.CurrentKET) > 0) THEN (1) ELSE (0) END as CurKET_t1_t7
		,CASE WHEN (sum(cd.CurrentMF) > 0) THEN (1) ELSE (0) END as CurMF_t1_t7
		,CASE WHEN (sum(cd.CurrentPRO) > 0) THEN (1) ELSE (0) END as CurPRO_t1_t7
		,CASE WHEN (sum(cd.CurrentLDA) > 0) THEN (1) ELSE (0) END as CurLDA_t1_t7
		,CASE WHEN (sum(cd.CurrentMAST) > 0) THEN (1) ELSE (0) END as CurMAST_t1_t7
		,CASE WHEN (sum(cd.CurrentEdma) > 0) THEN (1) ELSE (0) END as CurEdma_t1_t7
		,CASE WHEN (sum(cd.CurrentLAME) > 0) THEN (1) ELSE (0) END as CurLAME_t1_t7
		,CASE WHEN (sum(cd.CurrentRP) > 0) THEN (1) ELSE (0) END as CurRP_t1_t7
into cur_disease_t1_t7
from cow_calving_date ccd
left outer join cow_data cd
	on ccd.FarmCowID = cd.FarmCowID	and ccd.CalvingDate = DATEADD(day, -1, cd.Cur_Date)
where cd.Cur_Date between DATEADD(DAY, 0, ccd.CalvingDate ) and DATEADD(DAY, 7, ccd.CalvingDate )
group by ccd.FarmCowID, cd.CalvingDate
ORDER BY ccd.FarmCowID, cd.CalvingDate


-- find cows had Metritis from day 7 till day 30 - OUTCOME
drop table if exists cur_met_t7_t30

select distinct ccd.FarmCowID
		,cd.CalvingDate
		,CASE WHEN (sum(cd.CurrentMET) > 0) THEN (1) ELSE (0) END as CurMet_t7_t30
into cur_met_t7_t30
from cow_calving_date ccd
left outer join cow_data cd
	on ccd.FarmCowID = cd.FarmCowID	and ccd.CalvingDate = cd.CalvingDate
where cd.Cur_Date between DATEADD(day, 7, cd.CalvingDate ) and DATEADD(DAY, 30, cd.CalvingDate )
group by ccd.FarmCowID, cd.CalvingDate
ORDER BY ccd.FarmCowID, cd.CalvingDate

-- find cows relevant data before current calving
drop table if exists pre_calving_data

select   ccd.FarmCowID
		, ccd.CalvingDate
		, cd.DIM
		, cd.CurrentDryDays
		, cd.Fertility_Num
into pre_calving_data
from [dbo].[cow_calving_date] ccd
left outer join cow_data cd
	on ccd.FarmCowID = cd.FarmCowID and cd.Cur_Date = DATEADD(DAY, -1 , ccd.CalvingDate ) --take data 1 days before calving
order by   ccd.FarmCowID, ccd.CalvingDate



-------------------------------------------------------------------------------------------------------------
-- Creates the flat file by collecting all the rows from day after the calving 
-- removes all the rows with duplicates - CowID and CalvingDate appears more than one time

drop table if exists flat_file

select  [FarmCode]
		  ,[DateMonth]
		  ,pcd.DIM
		  ,cd7.[CowID]
		  ,cd7.FarmCowID
		  ,[BirthDate]
		  ,cd7.[CalvingDate]
		  ,[Cur_Date]
		  ,[CurrentConceivedDate]
		  ,[Breed]
		  ,[Sys_Status]
		  ,[Gynecology_Status]
		  ,[Herd_ID]
		  ,[Extended_Status]
		  ,[Heifer_Extend_Status]
		  ,[LactationNumber]
		  ,[IsPreviousLactation]
		  ,pcd.CurrentDryDays
		  ,[Is_Ready_to_breed]
		  ,pcd.Fertility_Num
		  ,[WeightCalv]
		  --,[CurrentRP]
		  --,[CurrentMET]
		  --,[CurrentKET]
		  --,[CurrentMF]
		  --,[CurrentPRO]
		  --,[CurrentLDA]
		  --,[CurrentMAST]
		  --,[CurrentEdma]
		  --,[CurrentLAME]
		  ,[Parity123]
		  --,[Age]
		  ,[Age_calc]
		  ,[Twin]
		  ,[Still]
		  ,[PrevDryDays]
		  ,[Term]
		  ,[PPUD]
		  --,[Group_ID]
		  --,[Group_ID_t1]
		  --,[Group_ID_t2]
		  --,[Group_ID_t3]
		  --,[Group_ID_t4]
		  --,[Group_ID_t5]
		  --,[Group_ID_t6]
		  --,[Group_ID_t7]
		  ,[Is_Milked]
		  ,[Is_Milked_t1]
		  ,[Is_Milked_t2]
		  ,[Is_Milked_t3]
		  ,[Is_Milked_t4]
		  ,[Is_Milked_t5]
		  ,[Is_Milked_t6]
		  ,[Is_Milked_t7]
		  ,[DailyYield_KG]
		  ,[DailyYield_KG_t1]
		  ,[DailyYield_KG_t2]
		  ,[DailyYield_KG_t3]
		  ,[DailyYield_KG_t4]
		  ,[DailyYield_KG_t5]
		  ,[DailyYield_KG_t6]
		  ,[DailyYield_KG_t7]
		  --,[TenDaysAvgYield]
		  --,[TenDaysAvgYield_t1]
		  --,[TenDaysAvgYield_t2]
		  --,[TenDaysAvgYield_t3]
		  --,[TenDaysAvgYield_t4]
		  --,[TenDaysAvgYield_t5]
		  --,[TenDaysAvgYield_t6]
		  ,[TenDaysAvgYield_t7]
		  --,[AccumulateYieldFromDay4]
		  --,[AccumulateYieldFromDay4_t1]
		  --,[AccumulateYieldFromDay4_t2]
		  --,[AccumulateYieldFromDay4_t3]
		  ,[AccumulateYieldFromDay4_t4]
		  ,[AccumulateYieldFromDay4_t5]
		  ,[AccumulateYieldFromDay4_t6]
		  ,[AccumulateYieldFromDay4_t7]
		  ,[DailyFat_P]
		  ,[DailyFat_P_t1]
		  ,[DailyFat_P_t2]
		  ,[DailyFat_P_t3]
		  ,[DailyFat_P_t4]
		  ,[DailyFat_P_t5]
		  ,[DailyFat_P_t6]
		  ,[DailyFat_P_t7]
		  --,[TenDaysAvgFat_P]
		  --,[TenDaysAvgFat_P_t1]
		  --,[TenDaysAvgFat_P_t2]
		  --,[TenDaysAvgFat_P_t3]
		  --,[TenDaysAvgFat_P_t4]
		  --,[TenDaysAvgFat_P_t5]
		  --,[TenDaysAvgFat_P_t6]
		  ,[TenDaysAvgFat_P_t7]
		  ,[DailyFat_KG]
		  ,[DailyFat_KG_t1]
		  ,[DailyFat_KG_t2]
		  ,[DailyFat_KG_t3]
		  ,[DailyFat_KG_t4]
		  ,[DailyFat_KG_t5]
		  ,[DailyFat_KG_t6]
		  ,[DailyFat_KG_t7]
		  --,[TenDaysAvgFat_KG]
		  --,[TenDaysAvgFat_KG_t1]
		  --,[TenDaysAvgFat_KG_t2]
		  --,[TenDaysAvgFat_KG_t3]
		  --,[TenDaysAvgFat_KG_t4]
		  --,[TenDaysAvgFat_KG_t5]
		  --,[TenDaysAvgFat_KG_t6]
		  ,[TenDaysAvgFat_KG_t7]
		  --,[AccumulateFatFromDay4_KG]
		  --,[AccumulateFatFromDay4_KG_t1]
		  --,[AccumulateFatFromDay4_KG_t2]
		  --,[AccumulateFatFromDay4_KG_t3]
		  ,[AccumulateFatFromDay4_KG_t4]
		  ,[AccumulateFatFromDay4_KG_t5]
		  ,[AccumulateFatFromDay4_KG_t6]
		  ,[AccumulateFatFromDay4_KG_t7]
		  ,[DailyProtein_P]
		  ,[DailyProtein_P_t1]
		  ,[DailyProtein_P_t2]
		  ,[DailyProtein_P_t3]
		  ,[DailyProtein_P_t4]
		  ,[DailyProtein_P_t5]
		  ,[DailyProtein_P_t6]
		  ,[DailyProtein_P_t7]
		  --,[TenDaysAvgProtein_p]
		  --,[TenDaysAvgProtein_p_t1]
		  --,[TenDaysAvgProtein_p_t2]
		  --,[TenDaysAvgProtein_p_t3]
		  --,[TenDaysAvgProtein_p_t4]
		  --,[TenDaysAvgProtein_p_t5]
		  --,[TenDaysAvgProtein_p_t6]
		  ,[TenDaysAvgProtein_p_t7]
		  ,[DailyProtein_KG]
		  ,[DailyProtein_KG_t1]
		  ,[DailyProtein_KG_t2]
		  ,[DailyProtein_KG_t3]
		  ,[DailyProtein_KG_t4]
		  ,[DailyProtein_KG_t5]
		  ,[DailyProtein_KG_t6]
		  ,[DailyProtein_KG_t7]
		  --,[TenDaysAvgProtein_KG]
		  --,[TenDaysAvgProtein_KG_t1]
		  --,[TenDaysAvgProtein_KG_t2]
		  --,[TenDaysAvgProtein_KG_t3]
		  --,[TenDaysAvgProtein_KG_t4]
		  --,[TenDaysAvgProtein_KG_t5]
		  --,[TenDaysAvgProtein_KG_t6]
		  ,[TenDaysAvgProtein_KG_t7]
		  --,[AccumulateProteinFromDay4_KG]
		  --,[AccumulateProteinFromDay4_KG_t1]
		  --,[AccumulateProteinFromDay4_KG_t2]
		  --,[AccumulateProteinFromDay4_KG_t3]
		  --,[AccumulateProteinFromDay4_KG_t4]
		  ,[AccumulateProteinFromDay4_KG_t5]
		  ,[AccumulateProteinFromDay4_KG_t6]
		  ,[AccumulateProteinFromDay4_KG_t7]
		  ,[ECM_KG]
		  ,[ECM_KG_t1]
		  ,[ECM_KG_t2]
		  ,[ECM_KG_t3]
		  ,[ECM_KG_t4]
		  ,[ECM_KG_t5]
		  ,[ECM_KG_t6]
		  ,[ECM_KG_t7]
		  ,[TenDaysAvgECM_KG]
		  ,[TenDaysAvgECM_KG_t1]
		  ,[TenDaysAvgECM_KG_t2]
		  ,[TenDaysAvgECM_KG_t3]
		  ,[TenDaysAvgECM_KG_t4]
		  ,[TenDaysAvgECM_KG_t5]
		  ,[TenDaysAvgECM_KG_t6]
		  ,[TenDaysAvgECM_KG_t7]
		  --,[AccumulateECMFromDay4_KG]
		  --,[AccumulateECMFromDay4_KG_t1]
		  --,[AccumulateECMFromDay4_KG_t2]
		  --,[AccumulateECMFromDay4_KG_t3]
		  ,[AccumulateECMFromDay4_KG_t4]
		  ,[AccumulateECMFromDay4_KG_t5]
		  ,[AccumulateECMFromDay4_KG_t6]
		  ,[AccumulateECMFromDay4_KG_t7]
		  ,[DailyConductivity]
		  ,[DailyConductivity_t1]
		  ,[DailyConductivity_t2]
		  ,[DailyConductivity_t3]
		  ,[DailyConductivity_t4]
		  ,[DailyConductivity_t5]
		  ,[DailyConductivity_t6]
		  ,[DailyConductivity_t7]
		  --,[TenDaysAvgConductivity]
		  --,[TenDaysAvgConductivity_t1]
		  --,[TenDaysAvgConductivity_t2]
		  --,[TenDaysAvgConductivity_t3]
		  --,[TenDaysAvgConductivity_t4]
		  --,[TenDaysAvgConductivity_t5]
		  --,[TenDaysAvgConductivity_t6]
		  ,[TenDaysAvgConductivity_t7]
		  ,[DailyProdRate]
		  ,[DailyProdRate_t1]
		  ,[DailyProdRate_t2]
		  ,[DailyProdRate_t3]
		  ,[DailyProdRate_t4]
		  ,[DailyProdRate_t5]
		  ,[DailyProdRate_t6]
		  ,[DailyProdRate_t7]
		  ,[DailyMilkingTime]
		  ,[DailyMilkingTime_t1]
		  ,[DailyMilkingTime_t2]
		  ,[DailyMilkingTime_t3]
		  ,[DailyMilkingTime_t4]
		  ,[DailyMilkingTime_t5]
		  ,[DailyMilkingTime_t6]
		  ,[DailyMilkingTime_t7]
		  ,[DailyActivity]
		  ,[DailyActivity_t1]
		  ,[DailyActivity_t2]
		  ,[DailyActivity_t3]
		  ,[DailyActivity_t4]
		  ,[DailyActivity_t5]
		  ,[DailyActivity_t6]
		  ,[DailyActivity_t7]
		  ,[DailyHeatIndicator]
		  ,[DailyHeatIndicator_t1]
		  ,[DailyHeatIndicator_t2]
		  ,[DailyHeatIndicator_t3]
		  ,[DailyHeatIndicator_t4]
		  ,[DailyHeatIndicator_t5]
		  ,[DailyHeatIndicator_t6]
		  ,[DailyHeatIndicator_t7]
		  ,[DailyRestRatio]
		  ,[DailyRestRatio_t1]
		  ,[DailyRestRatio_t2]
		  ,[DailyRestRatio_t3]
		  ,[DailyRestRatio_t4]
		  ,[DailyRestRatio_t5]
		  ,[DailyRestRatio_t6]
		  ,[DailyRestRatio_t7]
		  ,[DailyRestRestlessness]
		  ,[DailyRestRestlessness_t1]
		  ,[DailyRestRestlessness_t2]
		  ,[DailyRestRestlessness_t3]
		  ,[DailyRestRestlessness_t4]
		  ,[DailyRestRestlessness_t5]
		  ,[DailyRestRestlessness_t6]
		  ,[DailyRestRestlessness_t7]
		  ,[DailyRestPerBout]
		  ,[DailyRestPerBout_t1]
		  ,[DailyRestPerBout_t2]
		  ,[DailyRestPerBout_t3]
		  ,[DailyRestPerBout_t4]
		  ,[DailyRestPerBout_t5]
		  ,[DailyRestPerBout_t6]
		  ,[DailyRestPerBout_t7]
		  ,[DailyRestTime]
		  ,[DailyRestTime_t1]
		  ,[DailyRestTime_t2]
		  ,[DailyRestTime_t3]
		  ,[DailyRestTime_t4]
		  ,[DailyRestTime_t5]
		  ,[DailyRestTime_t6]
		  ,[DailyRestTime_t7]
		  ,[DailyRestBout]
		  ,[DailyRestBout_t1]
		  ,[DailyRestBout_t2]
		  ,[DailyRestBout_t3]
		  ,[DailyRestBout_t4]
		  ,[DailyRestBout_t5]
		  ,[DailyRestBout_t6]
		  ,[DailyRestBout_t7]
		  ,[DailyFPR_P]
		  ,[DailyFPR_P_t1]
		  ,[DailyFPR_P_t2]
		  ,[DailyFPR_P_t3]
		  ,[DailyFPR_P_t4]
		  ,[DailyFPR_P_t5]
		  ,[DailyFPR_P_t6]
		  ,[DailyFPR_P_t7]
		  --,[TenDaysAvgFPR_P]
		  --,[TenDaysAvgFPR_P_t1]
		  --,[TenDaysAvgFPR_P_t2]
		  --,[TenDaysAvgFPR_P_t3]
		  --,[TenDaysAvgFPR_P_t4]
		  --,[TenDaysAvgFPR_P_t5]
		  --,[TenDaysAvgFPR_P_t6]
		  ,[TenDaysAvgFPR_P_t7]
		  ,CASE WHEN (Group_ID <> Group_ID_t7) THEN (1) ELSE (0) END as GroupChanged
		,CurEdma_t1_t7
		,CurKET_t1_t7
		,CurLAME_t1_t7
		,CurLDA_t1_t7
		,CurMAST_t1_t7
		,CurMF_t1_t7
		,CurPRO_t1_t7
		,CurRP_t1_t7
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurEdma_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastEdma_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurKET_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastKET_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurLAME_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastLAME_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurLDA_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastLDA_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurMAST_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastMAST_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurMF_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastMF_t1_t30
		,CASE WHEN (LactationNumber = 1) THEN (0) ELSE (LAG(CurPRO_t1_t30) OVER(partition by ccd1.FarmCowID order by ccd1.CalvingDate)) END as LastPRO_t1_t30
		,CurMet_t7_t30
into flat_file
from (select  ccd.*, count(*) as cnt
		from cow_calving_date ccd
		left outer join  cow_data_t7 cd7
			on cd7.FarmCowID = ccd.FarmCowID and cd7.Cur_Date = DATEADD(day,1, ccd.CalvingDate )
		group by ccd.FarmCowID, ccd.CalvingDate
		having count(*) = 1) ccd1
inner join  cow_data_t7 cd7
	on cd7.FarmCowID = ccd1.FarmCowID and cd7.Cur_Date = DATEADD(day,1, ccd1.CalvingDate )
inner join cur_disease_t7_t30
	on cur_disease_t7_t30.FarmCowID = ccd1.FarmCowID and cur_disease_t7_t30.CalvingDate = ccd1.CalvingDate
inner join cur_disease_t1_t7
	on cur_disease_t1_t7.FarmCowID = ccd1.FarmCowID and cur_disease_t1_t7.CalvingDate = ccd1.CalvingDate
inner join cur_met_t7_t30
	on cur_met_t7_t30.FarmCowID = ccd1.FarmCowID and cur_met_t7_t30.CalvingDate = ccd1.CalvingDate
inner join pre_calving_data pcd
	on pcd.FarmCowID = ccd1.FarmCowID and pcd.CalvingDate = ccd1.CalvingDate
order by cd7.FarmCowID

--------------------------------------------------------------------------------------------------------------------
--TEST DATE----
--------------------------------------------------------------------------------------------------------------------
-- Number of cows
select count(*) as Cow_cnt
from (select distinct FarmCowID from cow_data_raw) a

-- Calvings in the period of the data 
select min(CalvingDate) as min_CalvingDate, min(Cur_Date) as min_Cur_Date
		,max(CalvingDate) as max_CalvingDate, max(Cur_Date) as max_Cur_Date
from cow_data

select count(*) as calvings_cnt
from (select distinct FarmCowID, CalvingDate 
		from cow_data 
		where CalvingDate >= (select min(Cur_Date) as min_Cur_Date 
								from cow_data)) a

select count(*) as cnt_duplicates
from (
	select  ccd.*, count(*) as duplicates_cnt
			from cow_calving_date ccd
			left outer join  cow_data_t7 cd7
				on cd7.FarmCowID = ccd.FarmCowID and cd7.Cur_Date = DATEADD(day,1, ccd.CalvingDate )
			group by ccd.FarmCowID, ccd.CalvingDate
			having count(*) > 1) a


select  count(*) as ff_rows_with_NULL
from (select  ccd.*, count(*) as cnt
		from cow_calving_date ccd
		left outer join  cow_data_t7 cd7
			on cd7.FarmCowID = ccd.FarmCowID and cd7.Cur_Date = DATEADD(day,1, ccd.CalvingDate )
		group by ccd.FarmCowID, ccd.CalvingDate
		having count(*) = 1) ccd
left outer join  cow_data_t7 cd7
	on cd7.FarmCowID = ccd.FarmCowID and cd7.Cur_Date = DATEADD(day,1, ccd.CalvingDate )

select count(*) as ff_rows_without_NULL, sum(CurMet_t7_t30) as countCurMet
from flat_file


GO