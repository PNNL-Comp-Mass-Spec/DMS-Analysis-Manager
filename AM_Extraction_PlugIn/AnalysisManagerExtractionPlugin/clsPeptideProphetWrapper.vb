'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2007, Battelle Memorial Institute
' Created 07/12/2007
'
' Program converted from original version written by J.D. Sandoval, PNNL.
' Conversion performed as part of upgrade to VB.Net 2005, modification for use with manager and broker databases
'
' Last modified 07/12/2007
' Modified for mini-pipeline by DAC - 09/24/2008
' Last modified 06/15/2009 JDS - Added logging using log4net
'*********************************************************************************************************
Imports PeptideProphetLibrary
Imports AnalysisManagerBase

Public Class clsPeptideProphetWrapper

	'*********************************************************************************************************
	' Wraps PeptideProphetLibrary.dll
	'*********************************************************************************************************

#Region "Module variables"
    Private m_ErrMsg As String = ""
    Private m_InputFile As String = ""
    Private m_OutputFilePath As String = ""
    Private m_Enzyme As String = ""

    Private m_PeptideProphet As PeptideProphetLibrary.PeptideProphet

#End Region

    Public Event PeptideProphetRunning(ByVal PepProphetStatus As String, ByVal PercentComplete As Single)

    Protected m_PepProphetThread As System.Threading.Thread
    Protected m_PepProphetThreadStart As New System.Threading.ThreadStart(AddressOf Me.StartPeptideProphet)
    Protected m_PepProphetRetVal As PeptideProphetLibrary.IPeptideProphet.ProcessStatus


#Region "Properties"
    Public ReadOnly Property ErrMsg() As String
        Get
            If m_ErrMsg Is Nothing Then
                Return String.Empty
            Else
                Return m_ErrMsg
            End If
        End Get
    End Property

    Public Property InputFile() As String
        Get
            Return m_InputFile
        End Get
        Set(ByVal Value As String)
            m_InputFile = Value
        End Set
    End Property

    Public Property Enzyme() As String
        Get
            Return m_Enzyme
        End Get
        Set(ByVal Value As String)
            m_Enzyme = Value
        End Set
    End Property

    Public Property OutputFilePath() As String
        Get
            Return m_OutputFilePath
        End Get
        Set(ByVal Value As String)
            m_OutputFilePath = Value
        End Set
    End Property
#End Region

#Region "Methods"

    Public Function CallPeptideProphet() As IJobParams.CloseOutType

        Const MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES As Integer = 120

        Dim dtStartTime As System.DateTime

        Try
            m_ErrMsg = String.Empty

            m_PepProphetThread = New System.Threading.Thread(m_PepProphetThreadStart)
            m_PepProphetThread.Start()
            dtStartTime = System.DateTime.Now

            ' Wait 2 seconds
            System.Threading.Thread.Sleep(2000)

            Do While m_PeptideProphet Is Nothing AndAlso System.DateTime.Now.Subtract(dtStartTime).TotalSeconds < 20
                ' Wait some more if the peptide prophet object still isn't instantiated
                System.Threading.Thread.Sleep(1000)
            Loop

            Do While (m_PeptideProphet.Status = PeptideProphetLibrary.IPeptideProphet.ProcessStatus.PP_STARTING) OrElse _
              (m_PeptideProphet.Status = PeptideProphetLibrary.IPeptideProphet.ProcessStatus.PP_RUNNING)
                System.Threading.Thread.Sleep(3000)

                RaiseEvent PeptideProphetRunning("Status = " & m_PeptideProphet.Status.ToString, m_PeptideProphet.PercentComplete)

                If System.DateTime.Now.Subtract(dtStartTime).TotalMinutes > MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES Then
                    m_PepProphetThread.Abort()
                    m_ErrMsg = "Peptide prophet has been running for over " & MAX_PEPTIDE_PROPHET_RUNTIME_MINUTES.ToString & " minutes; aborting"
                    Return IJobParams.CloseOutType.CLOSEOUT_FAILED
                End If
            Loop

            If m_PepProphetRetVal = IPeptideProphet.ProcessStatus.PP_ERROR Then
                m_ErrMsg = "Peptide prophet returned a non-zero error code: " & m_PepProphetRetVal.ToString

                Return IJobParams.CloseOutType.CLOSEOUT_FAILED
            End If

            RaiseEvent PeptideProphetRunning(IPeptideProphet.ProcessStatus.PP_COMPLETE.ToString, 100)

            Return IJobParams.CloseOutType.CLOSEOUT_SUCCESS

        Catch ex As System.Exception
            m_ErrMsg = "Exception while extracting files: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            m_PepProphetRetVal = Nothing
            Return IJobParams.CloseOutType.CLOSEOUT_FAILED
        End Try


    End Function

    Protected Sub StartPeptideProphet()

        ' Initialize peptide prophet
        m_PeptideProphet = New PeptideProphetLibrary.PeptideProphet

        Dim StartParams As PeptideProphetLibrary.InitializationParams
        StartParams = New PeptideProphetLibrary.InitializationParams

        StartParams.InputFileName = m_InputFile
        StartParams.OutputFilePath = m_OutputFilePath
        StartParams.Enzyme = m_Enzyme
        m_PeptideProphet.Setup(StartParams)

        Try
            'Call peptide prophet
            m_PepProphetRetVal = m_PeptideProphet.Start()

        Catch ex As System.Exception
            m_ErrMsg = "Error initializing Peptide Prophet: " & ex.Message & "; " & clsGlobal.GetExceptionStackTrace(ex)
            m_PepProphetRetVal = IPeptideProphet.ProcessStatus.PP_ERROR
        End Try
    End Sub
#End Region

End Class
