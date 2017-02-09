'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 12/18/2007
'
'*********************************************************************************************************

Imports AnalysisManagerBase
Imports System.IO
Imports System.Text.RegularExpressions

''' <summary>
''' This is the base class that implements a specific spectra file generator.
''' </summary>
''' <remarks></remarks>
Public MustInherit Class clsDtaGen
    Inherits clsEventNotifier
    Implements ISpectraFileProcessor

#Region "Module variables"
    Protected m_ErrMsg As String = String.Empty
    Protected m_WorkDir As String = String.Empty    ' Working directory on analysis machine
    Protected m_Dataset As String = String.Empty
    Protected m_RawDataType As clsAnalysisResources.eRawDataTypeConstants = clsAnalysisResources.eRawDataTypeConstants.Unknown

    Protected m_DtaToolNameLoc As String = String.Empty             ' Path to the program used to create DTA files

    Protected m_Status As ProcessStatus
    Protected m_Results As ProcessResults
    Protected m_MgrParams As IMgrParams
    Protected m_JobParams As IJobParams
    Protected m_DebugLevel As Short = 0
    Protected m_SpectraFileCount As Integer
    Protected m_StatusTools As IStatusFile

    Protected m_ToolRunner As clsAnalysisToolRunnerBase

    Protected m_AbortRequested As Boolean = False

    ' The following is a value between 0 and 100
    Protected m_Progress As Single = 0
#End Region

#Region "Properties"
    Public WriteOnly Property StatusTools() As IStatusFile Implements ISpectraFileProcessor.StatusTools
        Set(Value As IStatusFile)
            m_StatusTools = Value
        End Set
    End Property

    Public ReadOnly Property DtaToolNameLoc() As String Implements ISpectraFileProcessor.DtaToolNameLoc
        Get
            Return m_DtaToolNameLoc
        End Get
    End Property

    Public ReadOnly Property ErrMsg() As String Implements ISpectraFileProcessor.ErrMsg
        Get
            Return m_ErrMsg
        End Get
    End Property

    Public WriteOnly Property MgrParams() As IMgrParams Implements ISpectraFileProcessor.MgrParams
        Set(Value As IMgrParams)
            m_MgrParams = Value
        End Set
    End Property

    Public WriteOnly Property JobParams() As IJobParams Implements ISpectraFileProcessor.JobParams
        Set(Value As IJobParams)
            m_JobParams = Value
        End Set
    End Property

    Public ReadOnly Property Status() As ProcessStatus Implements ISpectraFileProcessor.Status
        Get
            Return m_Status
        End Get
    End Property

    Public ReadOnly Property Results() As ProcessResults Implements ISpectraFileProcessor.Results
        Get
            Return m_Results
        End Get
    End Property

    Public Property DebugLevel() As Integer Implements ISpectraFileProcessor.DebugLevel
        Get
            Return m_DebugLevel
        End Get
        Set(Value As Integer)
            m_DebugLevel = CShort(Value)
        End Set
    End Property

    Public ReadOnly Property SpectraFileCount() As Integer Implements ISpectraFileProcessor.SpectraFileCount
        Get
            Return m_SpectraFileCount
        End Get
    End Property

    Public ReadOnly Property Progress() As Single Implements ISpectraFileProcessor.Progress
        Get
            Return m_Progress
        End Get
    End Property
#End Region

#Region "Methods"
    ''' <summary>
    ''' Aborts processing
    ''' </summary>
    ''' <returns>ProcessStatus value indicating process was aborted</returns>
    ''' <remarks></remarks>
    Public Function Abort() As ProcessStatus Implements ISpectraFileProcessor.Abort
        m_AbortRequested = True
    End Function

    Public MustOverride Function Start() As ProcessStatus Implements ISpectraFileProcessor.Start

    Public Overridable Sub Setup(
      initParams As SpectraFileProcessorParams,
      toolRunner As clsAnalysisToolRunnerBase) Implements ISpectraFileProcessor.Setup

        ' Copies all input data required for plugin operation to appropriate memory variables
        With initParams
            m_DebugLevel = CShort(.DebugLevel)
            m_JobParams = .JobParams
            m_MgrParams = .MgrParams
            m_StatusTools = .StatusTools
            m_WorkDir = .WorkDir
            m_Dataset = .DatasetName
        End With

        m_ToolRunner = toolRunner

        m_RawDataType = clsAnalysisResources.GetRawDataType(m_JobParams.GetJobParameter("RawDataType", ""))

        m_Progress = 0

    End Sub

    Public Sub UpdateDtaToolNameLoc(progLoc As String)
        m_DtaToolNameLoc = progLoc
    End Sub

    Protected Function VerifyDirExists(TestDir As String) As Boolean

        ' Verifies that the specified directory exists
        If Directory.Exists(TestDir) Then
            m_ErrMsg = ""
            Return True
        Else
            m_ErrMsg = "Directory " & TestDir & " not found"
            Return False
        End If

    End Function

    Protected Function VerifyFileExists(TestFile As String) As Boolean
        ' Verifies specified file exists
        If File.Exists(TestFile) Then
            m_ErrMsg = ""
            Return True
        Else
            m_ErrMsg = "File " & TestFile & " not found"
            Return False
        End If

    End Function

    Protected Overridable Function InitSetup() As Boolean

        ' Initializes module variables and verifies mandatory parameters have been propery specified

        ' Manager parameters
        If m_MgrParams Is Nothing Then
            m_ErrMsg = "Manager parameters not specified"
            Return False
        End If

        ' Job parameters
        If m_JobParams Is Nothing Then
            m_ErrMsg = "Job parameters not specified"
            Return False
        End If

        ' Status tools
        If m_StatusTools Is Nothing Then
            m_ErrMsg = "Status tools object not set"
            Return False
        End If

        ' If we got here, everything's OK
        Return True

    End Function

    Protected Function DeleteNonDosFiles() As Boolean

        ' extract_msn.exe and lcq_dta.exe sometimes leave files with funky filenames containing non-DOS characters. 
        ' This function removes those files

        Dim workDir As New DirectoryInfo(m_WorkDir)

        Dim reValidFiles = New Regex(".dta$|.txt$|.csv$|.raw$|.params$|.wiff$|.xml$|.mgf$", RegexOptions.Compiled Or RegexOptions.IgnoreCase)

        For Each dataFile In workDir.GetFiles
            Dim reMatch = reValidFiles.Match(dataFile.Extension)
            If Not reMatch.Success Then
                Try
                    dataFile.Delete()
                Catch ex As Exception
                    m_ErrMsg = "Error removing non-DOS files: " & ex.Message
                    Return False
                End Try
            End If
        Next

        Return True

    End Function

    Protected Sub LogDTACreationStats(strProcedureName As String, strDTAToolName As String, strErrorMessage As String)

        Dim strMostRecentBlankDTA As String = String.Empty
        Dim strMostRecentValidDTA As String = String.Empty

        If strProcedureName Is Nothing Then
            strProcedureName = "clsDtaGen.??"
        End If
        If strDTAToolName Is Nothing Then
            strDTAToolName = "Unknown DTA Tool"
        End If

        If strErrorMessage Is Nothing Then
            strErrorMessage = "Unknown error"
        End If

        OnErrorEvent(strProcedureName & ", Error running " & strDTAToolName & "; " & strErrorMessage)

        ' Now count the number of .Dta files in the working folder

        Try
            Dim objFolderInfo = New DirectoryInfo(m_WorkDir)
            Dim objFiles = objFolderInfo.GetFiles("*.dta")
            Dim intDTACount As Integer

            If objFiles Is Nothing OrElse objFiles.Length <= 0 Then
                intDTACount = 0
            Else
                intDTACount = objFiles.Length

                Dim intMostRecentValidDTAIndex = -1
                Dim intMostRecentBlankDTAIndex = -1
                Dim lngDTAFileSize As Long = 0

                ' Find the most recently created .Dta file
                ' However, track blank (zero-length) .Dta files separate from those with data 
                For intIndex = 1 To objFiles.Length - 1
                    If objFiles(intIndex).Length = 0 Then
                        If intMostRecentBlankDTAIndex < 0 Then
                            intMostRecentBlankDTAIndex = intIndex
                        Else
                            If objFiles(intIndex).LastWriteTime > objFiles(intMostRecentBlankDTAIndex).LastWriteTime Then
                                intMostRecentBlankDTAIndex = intIndex
                            End If
                        End If
                    Else
                        If intMostRecentValidDTAIndex < 0 Then
                            intMostRecentValidDTAIndex = intIndex
                        Else
                            If objFiles(intIndex).LastWriteTime > objFiles(intMostRecentValidDTAIndex).LastWriteTime Then
                                intMostRecentValidDTAIndex = intIndex
                            End If
                        End If
                    End If
                Next

                If intMostRecentBlankDTAIndex >= 0 Then
                    strMostRecentBlankDTA = objFiles(intMostRecentBlankDTAIndex).Name
                End If

                If intMostRecentValidDTAIndex >= 0 Then
                    strMostRecentValidDTA = objFiles(intMostRecentValidDTAIndex).Name
                    lngDTAFileSize = objFiles(intMostRecentValidDTAIndex).Length
                End If

                If intDTACount > 0 Then
                    ' Log the name of the most recently created .Dta file
                    If intMostRecentValidDTAIndex >= 0 Then
                        OnStatusEvent(strProcedureName & ", The most recent .Dta file created is " & strMostRecentValidDTA &
                                      " with size " & lngDTAFileSize.ToString & " bytes")
                    Else
                        OnWarningEvent(strProcedureName & ", No valid (non zero length) .Dta files were created")
                    End If

                    If intMostRecentBlankDTAIndex >= 0 Then
                        OnStatusEvent(strProcedureName & ", The most recent blank (zero-length) .Dta file created is " & strMostRecentBlankDTA)
                    End If
                End If

            End If

            ' Log the number of .Dta files that were found
            OnStatusEvent(strProcedureName & ", " & strDTAToolName & " created " & intDTACount.ToString & " .dta files")

        Catch ex As Exception
            OnErrorEvent(", Error finding the most recently created .Dta file: " & ex.Message)
        End Try

    End Sub

#End Region

End Class
