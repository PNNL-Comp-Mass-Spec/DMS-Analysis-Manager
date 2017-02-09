'*********************************************************************************************************
' Written by Dave Clark for the US Department of Energy 
' Pacific Northwest National Laboratory, Richland, WA
' Copyright 12/18/2007
'
'*********************************************************************************************************

Option Strict On

Public Interface IMgrParams

    '*********************************************************************************************************
    'Interface for manager params storage class
    '*********************************************************************************************************

    ReadOnly Property ErrMsg As String

#Region "Methods"
    Sub AckManagerUpdateRequired()
    Function DisableManagerLocally() As Boolean

    ''' <summary>
    ''' Gets a parameter from the manager parameters dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for item</param>
    ''' <returns>String value associated with specified key</returns>
    ''' <remarks>Returns empty string if key isn't found</remarks>
    Function GetParam(ItemKey As String) As String

    ''' <summary>
    ''' Gets a parameter from the manager parameters dictionary
    ''' </summary>
    ''' <param name="ItemKey">Key name for item</param>
    ''' <param name="ValueIfMissing">Value to return if the parameter is not found</param>
    ''' <returns>Value for specified parameter; ValueIfMissing if not found</returns>
    Function GetParam(ItemKey As String, ValueIfMissing As String) As String
    Function GetParam(ItemKey As String, ValueIfMissing As Boolean) As Boolean
    Function GetParam(ItemKey As String, ValueIfMissing As Integer) As Integer

    Function LoadDBSettings() As Boolean
    Function LoadSettings(ConfigFileSettings As Dictionary(Of String, String)) As Boolean
    Sub SetParam(ItemKey As String, ItemValue As String)
#End Region

End Interface


