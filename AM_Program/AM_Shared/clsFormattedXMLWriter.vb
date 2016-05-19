'*********************************************************************************************************
' Written by Matt Monroe for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 2008, Battelle Memorial Institute
' Created 09/23/2008
'
' DAC modified for use with analysis manager 09/25/2008
'*********************************************************************************************************

Option Strict On

Imports System.IO
Imports System.Xml

Public Class clsFormattedXMLWriter

	'*********************************************************************************************************
	'Writes a formatted XML file
	'*********************************************************************************************************

#Region "Module variables"
	Dim m_ErrMsg As String
#End Region

#Region "Properties"
	Public ReadOnly Property ErrMsg() As String
		Get
			Return m_ErrMsg
		End Get
	End Property
#End Region

#Region "Methods"
    Public Function WriteXMLToFile(strXMLText As String, strOutputFilePath As String) As Boolean

        Dim objXMLDoc As XmlDocument
        Dim swOutfile As XmlTextWriter

        Dim blnSuccess As Boolean = False

        m_ErrMsg = ""

        Try
            ' Instantiate objXMLDoc
            objXMLDoc = New XmlDocument()
            objXMLDoc.LoadXml(strXMLText)

        Catch ex As Exception
            m_ErrMsg = "Error parsing the source XML text: " & ex.Message
            Return False
        End Try

        Try
            ' Initialize the XML writer
            swOutfile = New XmlTextWriter(New FileStream(strOutputFilePath, FileMode.Create, FileAccess.Write, FileShare.Read), Text.Encoding.UTF8)

            ' Define the indenting
            swOutfile.Formatting = Formatting.Indented
            swOutfile.Indentation = 2
            swOutfile.IndentChar = " "c

        Catch ex As Exception
            m_ErrMsg = "Error opening the output file (" & strOutputFilePath & ") in WriteXMLToFile: " & ex.Message
            Return False
        End Try

        Try
            ' Write out the XML
            objXMLDoc.WriteTo(swOutfile)
            swOutfile.Close()

            blnSuccess = True

        Catch ex As Exception
            m_ErrMsg = "Error in WritePepXMLFile: " & ex.Message
        Finally
            If Not swOutfile Is Nothing Then swOutfile.Close()
        End Try

        Return blnSuccess

    End Function
#End Region

End Class
