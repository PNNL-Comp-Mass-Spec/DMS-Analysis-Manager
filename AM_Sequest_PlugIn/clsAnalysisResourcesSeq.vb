Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.Collections.Specialized
Imports System.Text.RegularExpressions
Imports ParamFileGenerator.MakeParams
Imports AnalysisManagerBase
Imports AnalysisManagerMSMSResourceBase

Public Class clsAnalysisResourcesSeq
	Inherits clsAnalysisResourcesMSMS

	'*********************************************************************************************************
	'Subclass for Sequest-specific tasks:
	'	1) Distributes OrgDB files to cluster nodes if running on a cluster
	'	2) Uses ParamFileGenerator to create Sequest param file from database instead of copying it
	'*********************************************************************************************************

	Protected Overrides Function RetrieveOrgDB(ByVal LocOrgDBFolder As String) As Boolean

		'Provides a function that can copy the org db to the cluster head and distribute it to the nodes

		'Retrieve the OrgDb normally via base class
		If Not MyBase.RetrieveOrgDB(LocOrgDBFolder) Then Return False

		'If not running on a cluster, then exit. Otherwise, distribute database files across nodes
		If Not CBool(m_mgrParams.GetParam("sequest", "cluster")) Then Return True

		'Check the cluster nodes, updating local database copies as necessary
		If VerifyDatabase(m_jobParams.GetParam("generatedFastaName"), LocOrgDBFolder) Then
			Return True
		Else
			Return False
		End If

	End Function

	Protected Overrides Function RetrieveParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
	 ByVal WorkDir As String) As Boolean

		'Overrides base class version of the function to creates a Sequest params file compatible 
		'	with the Bioworks version on this system
		'Uses ParamFileGenerator dll provided by Ken Auberry
		'NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability

		Dim ParFileGen As IGenerateFile = New clsMakeParameterFile
		Dim Result As Boolean

		ParFileGen.TemplateFilePath = m_mgrParams.GetParam("sequest", "paramtemplateloc")

		Result = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion(m_mgrParams.GetParam("sequest", "bioworksversion")), _
		 Path.Combine(m_mgrParams.GetParam("commonfileandfolderlocations", "orgdbdir"), m_jobParams.GetParam("generatedFastaName")), _
		 WorkDir, m_mgrParams.GetParam("databasesettings", "connectionstring"))

		If Result Then
			Return True
		Else
			m_logger.PostEntry("Error converting param file: " & ParFileGen.LastError, ILogger.logMsgType.logError, True)
			Return False
		End If

	End Function

	Private Function VerifyDatabase(ByVal OrgDBName As String, ByVal OrgDBPath As String) As Boolean

		'Verifies the database required by the job is distributed to
		'	all the cluster nodes
		Dim HostFile As String = m_mgrParams.GetParam("cluster", "hostsfilelocation")
		Dim Nodes As StringCollection
		Dim NodeDbLoc As String = m_mgrParams.GetParam("cluster", "nodedblocation")

		m_logger.PostEntry("Copying Databases", ILogger.logMsgType.logNormal, True)

		'Get the list of nodes from the hosts file
		Nodes = GetHostList(HostFile)
		If Nodes Is Nothing Then Return False

		'For each node, verify specified database file is present and matches file on host
		For Each NodeName As String In Nodes
			If Not VerifyRemoteDatabase(OrgDBName, OrgDBPath, "\\" & NodeName & "\" & NodeDbLoc) Then
				Return False
			End If
		Next

		'Databases have been distributed, so return happy
		Return True

	End Function

	Private Function GetHostList(ByVal HostFileNameLoc As String) As StringCollection

		'Reads the list of nodes from the hosts config file, returns a string collection
		'	containing IP addresses for each node
		Dim NodeColl As New StringCollection
		Dim InpLine As String

		Try
			Dim HostFile As StreamReader = File.OpenText(HostFileNameLoc)
			InpLine = HostFile.ReadLine
			While Not InpLine Is Nothing
				'Read the line from the file and check to see if it contains a node IP address. If it does, add
				'	the IP address to the collection of addresses
				Dim TestExp As New Regex("\d+.\d+.\d+.\d+")
				Dim MatchCol As MatchCollection = TestExp.Matches(InpLine)
				If MatchCol.Count = 1 Then
					'This line has an IP address, so add it to the collection
					NodeColl.Add(MatchCol(0).Value)
				Else
					'No match on this line, so do nothing
				End If
				InpLine = HostFile.ReadLine
			End While
			HostFile.Close()
		Catch Err As Exception
			m_logger.PostError("Error reading cluster config file", Err, True)
			Return Nothing
		End Try

		'Return the list of nodes, if any
		If NodeColl.Count < 1 Then
			Return Nothing
		Else
			Return NodeColl
		End If

	End Function

	Private Function VerifyRemoteDatabase(ByVal DbName As String, ByVal SourcePath As String, _
	 ByVal DestPath As String) As Boolean

		'Verifies specified database is present on the node. If present, compares date and size. If not
		'	present, copies database from master
		'Assumes DestPath is URL containing IP address of node and destination share name

		Dim CopyNeeded As Boolean = False

		If m_DebugLevel > 3 Then
			m_logger.PostEntry("Verifying database " & DestPath, ILogger.logMsgType.logNormal, True)
		End If

		Dim SourceFile As String = Path.Combine(SourcePath, DbName)
		If Not File.Exists(SourceFile) Then
			m_logger.PostEntry("Database file can't be found on master", ILogger.logMsgType.logError, True)
			Return False
		End If

		Dim DestFile As String = Path.Combine(DestPath, DbName)
		Try
			If File.Exists(DestFile) Then
				'File was found on node, compare hash with copy on master
				If Not VerifyFastaVersion(DestFile, SourceFile) Then
					CopyNeeded = True
				End If
			Else
				'File wasn't on node, we'll have to copy it
				CopyNeeded = True
			End If

			'Does the file need to be copied to the node?
			If CopyNeeded Then
				'Copy the file
				If m_DebugLevel > 3 Then
					m_logger.PostEntry("Copying database file " & DestFile, ILogger.logMsgType.logNormal, True)
				End If
				File.Copy(SourceFile, DestFile, True)
				'Now everything is in its proper place, so return
				Return True
			Else
				'File existed and was current, so everybody's happy
				If m_DebugLevel > 3 Then
					m_logger.PostEntry("No copy required, database file " & DestFile, ILogger.logMsgType.logNormal, True)
				End If
				Return True
			End If
		Catch Err As Exception
			'Something bad happened
			m_logger.PostError("Error copying database file to node " & DestPath, Err, True)
			Return False
		End Try

	End Function

End Class
