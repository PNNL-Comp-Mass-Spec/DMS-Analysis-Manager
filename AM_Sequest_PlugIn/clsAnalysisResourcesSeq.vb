'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2006, Battelle Memorial Institute
' Created 06/07/06
'
' Last modified 02/14/2008
'*********************************************************************************************************

Imports System.IO
Imports PRISM.Logging
Imports PRISM.Files
Imports PRISM.Files.clsFileTools
Imports AnalysisManagerBase.clsGlobal
Imports System.Collections.Specialized
'Imports System.Text.RegularExpressions
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

#Region "Methods"
	''' <summary>
	''' Provides a function that can copy the org db to the cluster head and distribute it to the nodes
	''' </summary>
	''' <param name="LocOrgDBFolder">Folder on AM processing machine for orgdb files</param>
	''' <returns>TRUE for success: FALSE for failure</returns>
	''' <remarks></remarks>
	Protected Overrides Function RetrieveOrgDB(ByVal LocOrgDBFolder As String) As Boolean

		'Retrieve the OrgDb normally via base class
		If Not MyBase.RetrieveOrgDB(LocOrgDBFolder) Then Return False

		'If not running on a cluster, then exit. Otherwise, distribute database files across nodes
		If Not CBool(m_mgrParams.GetParam("cluster")) Then Return True

		'Check the cluster nodes, updating local database copies as necessary
		If VerifyDatabase(m_jobParams.GetParam("generatedFastaName"), LocOrgDBFolder) Then
			Return True
		Else
			Return False
		End If

	End Function

	''' <summary>
	''' Overrides base class version of the function to creates a Sequest params file compatible 
	'''	with the Bioworks version on this system. Uses ParamFileGenerator dll provided by Ken Auberry
	''' </summary>
	''' <param name="ParamFileName">Name of param file to be created</param>
	''' <param name="ParamFilePath">Param file storage path</param>
	''' <param name="WorkDir">Working directory on analysis machine</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>NOTE: ParamFilePath isn't used in this override, but is needed in parameter list for compatability</remarks>
	Protected Overrides Function RetrieveParamFile(ByVal ParamFileName As String, ByVal ParamFilePath As String, _
	  ByVal WorkDir As String) As Boolean

		Dim ParFileGen As IGenerateFile = New clsMakeParameterFile
		Dim Result As Boolean

		m_logger.PostEntry("Retrieving parameter file", ILogger.logMsgType.logNormal, True)

		ParFileGen.TemplateFilePath = m_mgrParams.GetParam("paramtemplateloc")
		Try
			Result = ParFileGen.MakeFile(ParamFileName, SetBioworksVersion(m_mgrParams.GetParam("bioworksversion")), _
			 Path.Combine(m_mgrParams.GetParam("orgdbdir"), m_jobParams.GetParam("generatedFastaName")), _
			 WorkDir, m_mgrParams.GetParam("connectionstring"), CInt(m_jobParams.GetParam("DatasetID")))
		Catch ex As Exception
			Dim Msg As String = "Exception generating param file: " & ex.Message
			m_logger.PostEntry(Msg, ILogger.logMsgType.logError, False)
			If ParFileGen.LastError IsNot Nothing Then
				m_logger.PostEntry("Error converting param file: " & ParFileGen.LastError, ILogger.logMsgType.logError, True)
			End If
			Return False
		End Try

		If Result Then
			Return True
		Else
			m_logger.PostEntry("Error converting param file: " & ParFileGen.LastError, ILogger.logMsgType.logError, True)
			Return False
		End If

	End Function

	''' <summary>
	''' Verifies the fasta file required by the job is distributed to all the cluster nodes
	''' </summary>
	''' <param name="OrgDBName">Fasta file name</param>
	''' <param name="OrgDBPath">Fasta file location on analysis machine</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks></remarks>
	Private Function VerifyDatabase(ByVal OrgDBName As String, ByVal OrgDBPath As String) As Boolean

		Dim HostFile As String = m_mgrParams.GetParam("hostsfilelocation")
		Dim Nodes As StringCollection
		Dim NodeDbLoc As String = m_mgrParams.GetParam("nodedblocation")

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

	''' <summary>
	''' Reads the list of nodes from the hosts config file
	''' </summary>
	''' <param name="HostFileNameLoc">Name of hosts file on cluster head node</param>
	''' <returns>returns a string collection containing IP addresses for each node</returns>
	''' <remarks></remarks>
	Private Function GetHostList(ByVal HostFileNameLoc As String) As StringCollection

		Dim NodeColl As New StringCollection
		Dim InpLine As String
		Dim LineFields() As String
		Dim Separators() As String = {" "}

		Try
			Dim HostFile As StreamReader = File.OpenText(HostFileNameLoc)
			InpLine = HostFile.ReadLine
			While Not InpLine Is Nothing

				''Read the line from the file and check to see if it contains a node IP address. If it does, add
				''	the IP address to the collection of addresses
				'Dim TestExp As New Regex("\d+.\d+.\d+.\d+")
				'Dim MatchCol As MatchCollection = TestExp.Matches(InpLine)
				'If MatchCol.Count = 1 Then
				'	'This line has an IP address, so add it to the collection
				'	NodeColl.Add(MatchCol(0).Value)
				'Else
				'	'No match on this line, so do nothing
				'End If
				'Verify the line isn't a comment line

				If InpLine.IndexOf("#") < 0 Then
					'Parse the node name and add it to the collection
					LineFields = InpLine.Split(Separators, StringSplitOptions.RemoveEmptyEntries)
					If LineFields.GetLength(0) >= 1 Then
						If NodeColl Is Nothing Then NodeColl = New StringCollection
						NodeColl.Add(LineFields(0))
					End If
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

	''' <summary>
	''' Verifies specified database is present on the node. If present, compares date and size. If not
	'''	present, copies database from master
	''' </summary>
	''' <param name="DbName">Fasta file name to be verified</param>
	''' <param name="SourcePath">Fasta storage location on cluster head</param>
	''' <param name="DestPath">Fasta storage location on cluster node</param>
	''' <returns>TRUE for success; FALSE for failure</returns>
	''' <remarks>Assumes DestPath is URL containing IP address of node and destination share name</remarks>
	Private Function VerifyRemoteDatabase(ByVal DbName As String, ByVal SourcePath As String, _
	  ByVal DestPath As String) As Boolean

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
				'					If Not VerifyFastaVersion(DestFile, SourceFile) Then		'Uncomment statement to re-enable verification before copy
				CopyNeeded = True
				'			End If		'Uncomment statement to re-enable verification before copy
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
#End Region

End Class
