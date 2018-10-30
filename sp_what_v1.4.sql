/*
--Original inspiration: https://simplesqlserver.com/tag/sys-dm_exec_requests/

/// On Blocks and SQL Statements:	///
Sometimes the most *recent* sql cmd does not reflect the source of the actual specific block object. 
If a series of cmds where executed within a batch, I will get the current statement, which may not be the source.
sys.dm_exec_connections / sys.dm_exec_requests (sql_handle).


/// inactive sessions ///
There is less data available (efficiently) for inactive sessions, at least without using sys.sysprocesses.
I have expiermented with a variety of other joins and other options to get the same data - but they sometimes exceed 1 second in duration, which is simply too long.

As sys.sysprocesses is deprecated, I am reluctant to build code around it.

Pity. :( 
*/


/************************************************************ Permissions Required for Normal users to call sp_what: ***********************************************
--If you want to allow access to other users/developers/specials to execute sp_what and see results, it will be neccesary to setup some special permissions first:

--Specifically, VIEW SERVER STATE and xp_logininfo.
--xp_logininfo (reccomended through a proxy) to allow sp_what to check if the input is a login.
--VIEW SERVER STATE to allow the user to see active sessions on the SQL instance.
--select permission on the msdb.dbo.sysjobs table.

--creating special snowflake user to test permissions:
USE [master]
GO
CREATE LOGIN [DidYouAssumeMyGender] WITH PASSWORD='ApacheHelicopter', DEFAULT_DATABASE=[master], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
GO
CREATE USER [DidYouAssumeMyGender] FOR LOGIN [DidYouAssumeMyGender]
GO
GRANT VIEW SERVER STATE TO [DidYouAssumeMyGender]
GO
GRANT EXECUTE ON sp_what TO [DidYouAssumeMyGender] 
GO
CREATE USER SpySpy WITHOUT LOGIN;
GO
GRANT EXECUTE ON xp_logininfo TO SpySpy
GO
USE msdb
GO
CREATE USER [DidYouAssumeMyGender] FOR LOGIN [DidYouAssumeMyGender]
GO
GRANT SELECT ON sysjobs TO [DidYouAssumeMyGender] 


--At this point, to allow your special and totally unique special user the ability to use sp_what, the following modification will need to take place:

************************************************************ End Permissions Required for Normal users to call sp_what: *******************************************/



--using master for easier reference. 
USE master
GO
IF OBJECT_ID('sp_what', 'P') IS NOT NULL
DROP PROC dbo.sp_what; 
GO
CREATE PROC dbo.sp_what --NULL,1,0
--DECLARE
	 @nvcWhat		NVARCHAR(1000)	= NULL
	,@iActive		BIT				= 1
	,@iTimes		INT				= 1
	
AS
/*
--Original inspiration: https://simplesqlserver.com/tag/sys-dm_exec_requests/

Author:	Nicholas Williams (nicholashenrywilliams@gmail.com)
Date:	October 2018
Desc:	WHAT JUST HAPPENED? OMG, NOOoooooOOOooooOOO!!!
		Displays helpful info on what is currently happening, with the pain of searching for the blocking root/head and dbcc inputbuffer combined.
		Also allows the filtering of sessions to either a spid, or a login, or a database name. (Active or inactive.)

Limitations:	If a process is a job from another server, the call to search for the job id will fail - hiding this session. Will fix.
				Also... prob need to hard code collation to get around some potential issues.
				Maybe include a min version as standard (with minimal columns returned?) and then a "max" version with more info if required.
				
https://tmblr.co/Z14uHt2ZjEyet


How to use:

Can be called on its own, without input and will display the active sessions, with any blocks.
Other inputs for the first parameter include:
Any valid login
Any Valid SPID
Any valid database

And the results will filter onto those sessions.

EXEC sp_what 'domain\login'		--includes data on all active sessions from this login.
EXEC sp_what 'domain\login',0	--includes data on all sessions (inactive and active) from this login.
EXEC sp_what 115				--includes data on the session id 115
EXEC sp_what 'master'			--includes all active sessions that are connected to the msdb database.
EXEC sp_what 'msdb', 0			--includes all sessions (inactive and active) that are connected to the msdb database.
EXEC sp_what 'msdb', 0, 5		--Executes the search 5 times, with a 0.5 delay per search, then reports on all data captured. (in this case all session for the msdb database.)


Included is the option to include or exclude only active sessions - and the option to run it multiple times and collect the results over a 
period of time.

I like to save sp_what to my keyboard shortcuts of ctrl+3.
Its fun to highlight a string with a login name, or a spid and hit ctrl+3... and watch the developers faces as they try to see how
a string or a spid can be sent to the same input. lol.*

*yes, i know this is sad. I get my laughs where i can.
*/

SET NOCOUNT ON
BEGIN TRY
DECLARE 
	 @iRun			INT				= 0
	,@iSession_id	INT				= NULL
	,@nvcSQLExec	NVARCHAR(MAX)
	,@nvcSQLSuffix	NVARCHAR(MAX)
	,@nvcSQLPreffix	NVARCHAR(MAX)
	,@ncvDatabase	NVARCHAR(1000)
	,@nvcLogin		NVARCHAR(1000)
	,@nvcERR_MSG	NVARCHAR(4000)
    ,@iERR_SEV		SMALLINT
    ,@iERR_STA		SMALLINT

IF OBJECT_ID('tempdb.dbo.#tlb_UnicornsTasteGoodWhenFried', 'U') IS NOT NULL
DROP TABLE #tlb_UnicornsTasteGoodWhenFried; 

CREATE TABLE #tlb_UnicornsTasteGoodWhenFried
(
	 counter					INT
	,session_id					INT
	,blocking					INT
	,BlockingHead				INT
	,BlockedBy					INT
	,[DD:HH:MM:SS]				VARCHAR(14)	
	,Active						VARCHAR(3)
	,status						VARCHAR(20)
	,Threads					INT
	,Statement					VARCHAR(MAX)
	,Query						VARCHAR(MAX)
	,database_name				VARCHAR(254)	
	,Pct_Comp					INT
	,Comp_Time					VARCHAR(20)
	,Wait_Time_Sec				DECIMAL(20,3)
	,wait_resource				VARCHAR(100)	
	,CPU_Sec					DECIMAL(20,3)
	,Reads_K					DECIMAL(20,3)
	,Writes_K					DECIMAL(20,3)
	,login_time					DATETIME
	,host_name					VARCHAR(100)
	,program_name				VARCHAR(100)	
	,login_name					VARCHAR(100)
	,last_request_start_time	DATETIME
	,last_request_end_time		DATETIME
)

IF @nvcWhat = '' OR RTRIM(LTRIM(@nvcWhat)) = ''
BEGIN
	SET @nvcWhat = NULL 
END 

IF @nvcWhat IS NULL
BEGIN
	GOTO FlyBabyFly
END

--is input a valid number? (spid)
IF ISNUMERIC(@nvcWhat)  >= 1
BEGIN
	SET @iSession_id = CAST(@nvcWhat as INT)
	GOTO FlyBabyFly
END 

--if there is a string which is both a database and a login... then this code will default to the database as its standard, and ignore the login. cuz databases > people. *totally not anti-social. no, really! *
IF (SELECT TOP 1 name FROM sys.databases WHERE name = @nvcWhat) IS NOT NULL
BEGIN
	SET		@ncvDatabase = @nvcWhat
	GOTO	FlyBabyFly
END

--this section is to determine if the input paramater is a valid login. This should only run if the input is not a valid number and if the input is not a database.
IF (SELECT TOP 1 name FROM sys.server_principals WHERE name = @nvcWhat) IS NULL
	BEGIN
		--EXECUTE as user = 'SpySpy'
		DECLARE @tbl TABLE
		(
			 [Account name]			sysname	NULL
			,[type]					char(8)	NULL
			,[privilege]			char(9) NULL
			,[mapped login name]	sysname	NULL
			,[permission path]		sysname	NULL
		)

		INSERT INTO @tbl ([Account name],[type],[privilege],[mapped login name],[permission path])
		EXEC master..xp_logininfo @nvcWhat, 'all';

		IF (SELECT TOP 1 [Account name]  FROM @tbl WHERE [Account name] = @nvcWhat) IS NULL
			BEGIN 
			 --print 'invalid login, so use null as an entry for checking'
			 SET @iSession_id = NULL 
			 GOTO FlyBabyFly
			END
				ELSE
				BEGIN
					SET @nvcLogin = @nvcWhat
					--print 'valid login, so check all sessions for that login...'
				END
		--REVERT
		GOTO FlyBabyFly
	END
	ELSE
		BEGIN
			--valid login from sys.server_principals
			SET @nvcLogin = @nvcWhat 
		END
		
/*
Did you know that the joke about a chicken crossing the road.... (to get to the other *SIDE*) is about suicide? True story. Life Changed.*
*this was also a joke.
*/

FlyBabyFly:
--PRINT 'Baby did fly. no input, so default to null and skip other checks.'

SET @nvcSQLExec =
N'
;WITH a AS 
	(
	SELECT 
		 es.session_id
		,es.is_user_process
		,CASE 
			WHEN er.sql_handle IS NULL 
			THEN cn.[most_recent_sql_handle]
			ELSE er.sql_handle 
			END sql_handle
		,ot.Threads
		,er.percent_complete	Pct_Comp 
		,CASE er.estimated_completion_time 
			WHEN 0 
			THEN NULL 
			ELSE dateadd(ms,er.estimated_completion_time,GETDATE()) 
			END  Comp_Time
		,es.status
		,CASE 
			WHEN es.[status] IN (''sleeping'',''dormant'') 
			THEN ''No*'' 
			ELSE ''Yes'' 
			END as [Active]
		,ISNULL(er.blocking_session_id, 0) BlockedBy
		,er.command
		,sd.name database_name 
		,CAST(er.wait_time/1000.0 as DEC(20,3))	Wait_Time_Sec
		,er.wait_resource
		,CASE 
			WHEN er.[total_elapsed_time] IS NULL
			THEN CONVERT(varchar,CAST(((DATEDIFF(ss, login_time, GETDATE())) / 86400) as INT )) + '':'' + CONVERT(varchar,DATEADD(ss,(DATEDIFF(ss, login_time, GETDATE())),0),108) 
			ELSE CONVERT(varchar,CAST(((er.[total_elapsed_time] / 1000.0) / 86400) as INT )) + '':'' + CONVERT(varchar,DATEADD(ss,(er.[total_elapsed_time] / 1000.0),0),108) 
		 END [DD:HH:MM:SS]
		,CAST(er.cpu_time/1000.0 as DEC(20,3))	CPU_Sec
		,CAST(er.reads/1000.0 as DEC(20,3))	Reads_K
		,CAST(er.writes/1000.0 as DEC(20,3))	Writes_K
		,es.login_time
		,es.host_name
		,CASE LEFT(es.program_name,29)
			WHEN ''SQLAgent - TSQL JobStep (Job ''
			THEN ''SQLAgent Job: '' + (SELECT name FROM msdb..sysjobs sj WHERE SUBSTRING(es.program_name,32,32)=(SUBSTRING(sys.fn_varbintohexstr(sj.job_id),3,100))) + '' - '' + SUBSTRING(es.program_name,67,len(es.program_name)-67)
			ELSE es.program_name
			END  program_name
		,es.client_interface_name
		,es.login_name
		,es.total_scheduled_time
		,es.total_elapsed_time
		,er.start_time
		,es.last_request_start_time
		,es.last_request_end_time
		,er.database_id  
		,er.statement_end_offset 
		,er.statement_start_offset
	FROM		sys.dm_exec_sessions	es
	LEFT JOIN	sys.dm_exec_requests	er	ON	es.session_id	=	er.session_id
	LEFT JOIN	sys.databases			sd	ON	er.database_id	=	sd.database_id
	LEFT JOIN	sys.dm_exec_connections	cn	ON	es.session_id	=	cn.session_id
	LEFT JOIN	(SELECT session_id,COUNT(1) Threads FROM sys.dm_os_tasks GROUP BY session_id) ot ON er.session_id=ot.session_id
	WHERE		es.session_id <> @@SPID
	AND			es.is_user_process = 1
	)
,b AS 
	(
	SELECT 
		CASE 
			WHEN session_id IN (SELECT DISTINCT BlockedBy FROM a WHERE a.BlockedBy IS NOT NULL AND BlockedBy <> 0)
			THEN 1
			ELSE 0
		 END blocking
		,a.*
	FROM a
	)
,c AS
	(
	SELECT 
		 NULL counter
		,session_id
		,blocking
		,CASE 
			WHEN blocking = 1 AND BlockedBy = 0
			THEN session_id
			ELSE NULL
		 END BlockingHead
		,BlockedBy
		,[DD:HH:MM:SS]
		,Active
		,status
		,Threads
		,SUBSTRING	(st.text, b.statement_start_offset/2,
					ABS(CASE 
						WHEN b.statement_end_offset = -1
						THEN LEN(CONVERT(NVARCHAR(MAX), st.text)) * 2 
						ELSE b.statement_end_offset 
						END - b.statement_start_offset
					)/2
					) [Statement] 
		,st.text Query
		,database_name
		,Pct_Comp
		,Comp_Time
		,Wait_Time_Sec
		,wait_resource
		,CPU_Sec
		,Reads_K
		,Writes_K
		,login_time
		,host_name
		,program_name
		,login_name
		,last_request_start_time
		,last_request_end_time
	FROM b 
	CROSS APPLY	sys.dm_exec_sql_text(b.[sql_handle]) AS st  
	WHERE 1=1 --makes it easier to add/remove conditions within dynamic SQL.
'
/*
These if clauses... exactly what I intended. 
*cough* 
https://9gag.com/gag/a4Q4RXZ
*/

IF @ncvDatabase IS NOT NULL AND (@iActive <> 0 OR @iActive IS NULL)
BEGIN
	SET @nvcSQLExec = @nvcSQLExec + N' AND database_name = ''' + CAST(@ncvDatabase as NVARCHAR(255))
	+'''
	AND (Active <> ''No*'' OR blocking = 1) '
	PRINT @nvcSQLExec
END

IF @ncvDatabase IS NOT NULL AND @iActive = 0 
BEGIN
	SET @nvcSQLExec = @nvcSQLExec + N' AND database_name = ''' + CAST(@ncvDatabase as NVARCHAR(255))+''''
	PRINT @nvcSQLExec
END

IF @nvcLogin IS NOT NULL AND (@iActive <> 0 OR @iActive IS NULL)
BEGIN
	SET @nvcSQLExec = @nvcSQLExec + N' AND login_name = ''' + CAST(@nvcLogin as NVARCHAR(255)) +'''
	AND (Active <> ''No*'' OR blocking = 1) '
	PRINT @nvcSQLExec
END

IF @nvcLogin IS NOT NULL AND @iActive = 0 
BEGIN 
	SET @nvcSQLExec = @nvcSQLExec + N' AND login_name = ''' + CAST(@nvcLogin as NVARCHAR(255))+''''
	--PRINT @nvcSQLExec
END

IF @iSession_id IS NOT NULL
BEGIN 
	SET @nvcSQLExec = @nvcSQLExec + N' AND session_id = ' + CAST(@iSession_id as NVARCHAR(255))
	--PRINT @nvcSQLExec
END

IF @iSession_id IS NULL AND (@iActive <> 0 OR @iActive IS NULL)
BEGIN 
	SET @nvcSQLExec = @nvcSQLExec + N' AND (Active <> ''No*'' OR blocking = 1) ' 
	--PRINT @nvcSQLExec
END

IF @iSession_id IS NULL AND @iActive = 0
BEGIN 
	SET @nvcSQLExec = @nvcSQLExec + ' ' 
	--PRINT @nvcSQLExec
END

--suffix
SET @nvcSQLExec = @nvcSQLExec + N' ) INSERT INTO #tlb_UnicornsTasteGoodWhenFried SELECT * FROM c'


IF (@iTimes < 1 OR @iTimes > 100) OR @iTimes IS NULL  SET @iTimes = 1  

IF @iTimes > 1 
BEGIN 
	WHILE @iRun < @iTimes
	BEGIN 
		SET @iRun=@iRun+1

		--PRINT @nvcSQLExec
		EXEC sp_executesql @nvcSQLExec;

		UPDATE #tlb_UnicornsTasteGoodWhenFried
		SET counter = @iRun
		WHERE counter IS NULL;

		WAITFOR DELAY '00:00:00.5'
	END
END

IF @iTimes = 1 
BEGIN
	--PRINT @nvcSQLExec
	EXEC sp_executesql @nvcSQLExec
END

SELECT 
	 GETDATE()		[Time]
	,@@SERVERNAME	[SQLInstance]
	,SERVERPROPERTY('ComputerNamePhysicalNetBIOS') [Node]

/*
https://9gag.com/gag/a9Kdj56
*/
IF EXISTS (SELECT TOP 1 * FROM #tlb_UnicornsTasteGoodWhenFried) 
BEGIN
	IF @iTimes = 1
	BEGIN
		IF EXISTS (SELECT TOP 1 * FROM #tlb_UnicornsTasteGoodWhenFried WHERE BlockingHead IS NOT NULL) 
		BEGIN	
			SELECT DISTINCT 
				 session_id
				,blocking
				,BlockingHead
				,BlockedBy
				,[DD:HH:MM:SS]
				,Active
				,[status]
				,[Statement]
				,Query
				,[database_name]
				,login_name
				,CPU_Sec
				,Reads_K
				,Writes_K
				,[host_name]
				,[program_name]
				,login_time
		--		,last_request_start_time
				,last_request_end_time
				,Threads
				,Wait_Time_Sec
				,wait_resource
				,Pct_Comp
		--		,Comp_Time
			FROM #tlb_UnicornsTasteGoodWhenFried
		END
		ELSE
		BEGIN
				SELECT DISTINCT
					 session_id
					--,blocking
					--,BlockingHead
					--,BlockedBy
					,[DD:HH:MM:SS]
					,Active
					,[status]
					,[Statement]
					,Query
					,[database_name]
					,login_name
					,CPU_Sec
					,Reads_K
					,Writes_K
					,[host_name]
					,[program_name]
					,login_time
			--		,last_request_start_time
					,last_request_end_time
					,Threads
					,Wait_Time_Sec
					,wait_resource
					,Pct_Comp
			--		,Comp_Time
				FROM #tlb_UnicornsTasteGoodWhenFried
		END
	END
	ELSE
	BEGIN
		SELECT 
		 [counter]
		,session_id
		,blocking
		,BlockingHead
		,BlockedBy
		,[DD:HH:MM:SS]
		,Active
		,[status]
		,[Statement]
		,Query
		,[database_name]
		,login_name
		,CPU_Sec
		,Reads_K
		,Writes_K
		,[host_name]
		,[program_name]
		,login_time
--		,last_request_start_time
		,last_request_end_time
		,Threads
		,Wait_Time_Sec
		,wait_resource
		,Pct_Comp
--		,Comp_Time
	FROM #tlb_UnicornsTasteGoodWhenFried

	END
END

IF (SELECT TOP 1 blocking FROM #tlb_UnicornsTasteGoodWhenFried where blocking <> 0 ) IS NOT NULL
BEGIN
	SELECT DISTINCT
		 session_id
		,BlockingHead 
		,[DD:HH:MM:SS]
		,login_time
		,Query
		,Statement
		,database_name
		,login_name
		,program_name
		,host_name
	FROM #tlb_UnicornsTasteGoodWhenFried
	WHERE BlockingHead IS NOT NULL
END

IF @iSession_id < 0
BEGIN  
	IF EXISTS (SELECT TOP 1 * FROM sys.dm_tran_locks WHERE request_session_id < 0) 
	BEGIN
		SELECT 
			 'KILL ' + CAST(request_owner_guid as VARCHAR) KillCmd
			,*
		FROM sys.dm_tran_locks
		WHERE request_session_id < 0
		AND request_owner_guid	<>'00000000-0000-0000-0000-000000000000' 
	END
END

/*
https://tmblr.co/Z14uHt2ZfI-kN
*/
DROP TABLE #tlb_UnicornsTasteGoodWhenFried
END TRY
BEGIN CATCH  
	SELECT  
		 @iERR_SEV		= ERROR_SEVERITY()
		,@iERR_STA		= ERROR_STATE()		
		,@nvcERR_MSG	= ERROR_MESSAGE()
--	THROW
	RAISERROR (@nvcERR_MSG, @iERR_SEV, @iERR_STA)  WITH NOWAIT
END CATCH

SET NOCOUNT OFF
