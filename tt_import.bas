Attribute VB_Name = "Module1"
' zero degrees is at 3 o'clock for arc positions
' zero degrees is at 12 o'clock for rotations
'
' MsoShapeType 1 is an autoshape
'
' MsoAutoShapeType 20 is MsoShapeBlockArc
'   Adjustments(1) is degrees of leading edge clockwise
'   Adjustments(2) is degrees of trailing edge clockwise
'   Adjustments(3) is width of arc line as a percent of the width of the arc in points. e.g. for 0.25 inches = 72*0.25/shape.width
'
' MsoAutoShapeType 1 is an MsoShapeRectangle
'
' MsoShapeType 6 is MsoShapeGroup
'

Public arc_degree_offset As Double
Public rotation_degree_offset As Double
Public p_color As Variant
Public n_color As Variant
Public min_color As Variant
Public slide_name_date_lookup As Object
Public slide_name_arc_start_lookups As Object
Public slide_name_arc_end_lookups As Object
Public slide_name_shape_name_shape_lookups As Object
Public file_dir As String

Public name_start_lookup As Object
Public name_end_lookup As Object

Sub main()

    Call set_globals
    Call set_default

    ' collect up the input files
    file_filter = "*.csv"
    
    ' set file interator "Dir"
    file = Dir(file_dir & file_filter)
    file_fields = Split(file, "_")
    chart_year = file_fields(0)
    create_slides (chart_year)

    ' interate files
    Do Until file = ""
    
        Debug.Print "Processing " & file
        file_path = file_dir & file
        file_num = FreeFile
        file_name_fields = Split(Split(file, ".")(0), "_")
        chart_year = file_name_fields(0)
        route_heading = CInt(file_name_fields(1))
        entity = file_name_fields(2)
        
        Call update_master(route_heading, chart_year)
    
        Open file_path For Input Access Read As file_num
        Line Input #file_num, Line
        headers = Split(Line, ",")

        previous_date = "1/1/1970"
        Set this_row = CreateObject("Scripting.Dictionary")

        While Not EOF(file_num)
            this_row.RemoveAll
            Line Input #file_num, Line
            line_fields = Split(Line, ",")
            
            For i = 0 To UBound(line_fields)
                this_row.Add headers(i), line_fields(i)
            Next
            
            slide_name = Format(DateValue(this_row("date")), "mmmm d yy")
            shape_name = this_row("name")
            
            Set this_slide = ActivePresentation.Slides(slide_name)
            
            If entity = "arcs" Then
                Set this_shape = this_slide.Shapes(shape_name)
                this_shape.Adjustments(1) = arc_degree_offset + this_row("start")
                this_shape.Adjustments(2) = arc_degree_offset + this_row("end")
                this_shape.Visible = True
            End If
            
            If entity = "minima" Then
                Set this_shape = this_slide.Shapes(shape_name)
                this_shape.Rotation = rotation_degree_offset + this_row("angle")
                this_shape.Visible = True
            End If
            
            If entity = "legend" Then
                Set this_shape = this_slide.Shapes("speed legend")
                this_shape.Rotation = this_row("angle")
                this_shape.Visible = True
            End If

        Wend
        Close
        file = Dir
    Loop
    
    If MsgBox("Export images?", vbYesNo) = vbYes Then Call save_images
    
End Sub

Sub set_default()

    Dim shp As Shape
    
    Set lookup = CreateObject("Scripting.Dictionary")
    For Speed = 3 To 7 Step 2
        For Count = 0 To 3
            lookup.Add "start P" & Str(Speed) & " A" & Str(Count + 1), 0 + 90 * Count
            lookup.Add "end P" & Str(Speed) & " A" & Str(Count + 1), 15 + 90 * Count
            lookup.Add "start N" & Str(Speed) & " A" & Str(Count + 1), 45 + 90 * Count
            lookup.Add "end N" & Str(Speed) & " A" & Str(Count + 1), 60 + 90 * Count
            lookup.Add "P" & Str(Speed) & Str(Count + 1) & " min", 7.5 + 90 * Count
            lookup.Add "N" & Str(Speed) & Str(Count + 1) & " min", 52.5 + 90 * Count
         Next
    Next
        
    Set this_slide = ActivePresentation.Slides(1)
    this_slide.Name = "Base Slide"
    this_slide.Shapes("speed legend").Visible = False
    this_slide.Shapes("Day Divider").Visible = True
    
    ActivePresentation.SlideMaster.CustomLayouts(7).Shapes("Year").TextFrame.TextRange.Text = 1970
    Set shp = ActivePresentation.SlideMaster.CustomLayouts(7).Shapes("Compass Needle")
    shp.Rotation = 0
    For Each member In shp.GroupItems
        If member.Name = "needle P" Then
            member.Fill.ForeColor.RGB = p_color
        ElseIf member.Name = "needle N" Then
            member.Fill.ForeColor.RGB = n_color
        End If
    Next
    
    For Each shp In this_slide.Shapes
        If shp.AutoShapeType = 20 Then
            shp.Adjustments(1) = arc_degree_offset + lookup("start " & shp.Name)
            shp.Adjustments(2) = arc_degree_offset + lookup("end " & shp.Name)
            shp.Adjustments(3) = 72 * 0.25 / shp.Width
            shp.Rotation = 0
            If InStr(shp.Name, "P") Then
                shp.Fill.ForeColor.RGB = p_color
            ElseIf InStr(shp.Name, "N") Then
                shp.Fill.ForeColor.RGB = n_color
            End If
        End If
        If shp.Type = 6 Then
            shp.Rotation = rotation_degree_offset + lookup(shp.Name)
            For Each member In shp.GroupItems
                If member.AutoShapeType = 7 Then
                    member.Fill.ForeColor.RGB = min_color
                    member.Line.ForeColor.RGB = min_color
                End If
            Next
        End If
    Next

End Sub

Sub create_slides(chart_year)

    Dim lookup As Object

    Debug.Print "creating slides"

    Set base_slide = ActivePresentation.Slides("Base Slide")

    slide_date = DateValue("1/1/" & chart_year)
    slide_year = year(slide_date)
    
    While year(slide_date) = slide_year
        Set new_slide = base_slide.Duplicate
        new_slide.Shapes("Day of Week").TextFrame.TextRange.Text = Format(slide_date, "dddd")
        new_slide.Shapes("Month and Number").TextFrame.TextRange.Text = Format(slide_date, "d mmmm ")
        new_slide.Shapes("Day of Week").Visible = True
        new_slide.Shapes("Month and Number").Visible = True
        new_slide.Name = Format(slide_date, "mmmm d yy")
        
        slide_name_date_lookup.Add new_slide.Name, slide_date
        slide_date = DateAdd("d", 1, slide_date)
        
        slide_name_arc_start_lookups.Add new_slide.Name, CreateObject("Scripting.Dictionary")
        slide_name_arc_end_lookups.Add new_slide.Name, CreateObject("Scripting.Dictionary")
        slide_name_shape_name_shape_lookups.Add new_slide.Name, CreateObject("Scripting.Dictionary")
        
        Set lookup = slide_name_shape_name_shape_lookups(new_slide.Name)
        For Each shp In new_slide.Shapes
            lookup.Add shp.Name, shp
        Next
        
    Wend
    
End Sub

Sub set_globals()

    Debug.Print "setting globals"
    
    arc_degree_offset = -180
    rotation_degree_offset = -90
    p_color = RGB(0, 153, 255)
    n_color = RGB(204, 153, 0)
    min_color = RGB(255, 255, 255)
    Set slide_name_date_lookup = CreateObject("Scripting.Dictionary")
    Set slide_name_arc_start_lookups = CreateObject("Scripting.Dictionary")
    Set slide_name_arc_end_lookups = CreateObject("Scripting.Dictionary")
    Set slide_name_shape_name_shape_lookups = CreateObject("Scripting.Dictionary")
    file_dir = Environ("Userprofile") & "\Developer Workspace\East River\Transit Time\"

    
End Sub

Sub save_images()

    Set fs = CreateObject("Scripting.FileSystemObject")
    image_dir = file_dir & "images"
    If fs.folderexists(image_dir) Then fs.deletefolder image_dir
    MkDir image_dir

    For Each Slide In ActivePresentation.Slides
        If Slide.Name <> "Base Slide" Then
            image_type = "png"
            image_path = image_dir & "\" & Format(Slide.Name, "mm-dd-yy") & "." & image_type
            Debug.Print image_path
            Slide.Export image_path, image_type
        End If
    Next
    
End Sub


Sub update_master(heading, year)

    Debug.Print "updating slide master"

    ActivePresentation.SlideMaster.CustomLayouts(7).Shapes("Year").TextFrame.TextRange.Text = year
    ActivePresentation.SlideMaster.CustomLayouts(7).Shapes("Compass Needle").Rotation = heading - 90

    Set shp = ActivePresentation.SlideMaster.CustomLayouts(7).Shapes("Compass Needle")
    For Each member In shp.GroupItems
        If member.Name = "needle P" Then
            member.Fill.ForeColor.RGB = p_color
        ElseIf member.Name = "needle N" Then
            member.Fill.ForeColor.RGB = n_color
        End If
    Next

End Sub
