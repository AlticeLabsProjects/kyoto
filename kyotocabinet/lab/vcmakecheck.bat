@echo off
rem ================================================================
rem vcmakecheck.bat
rem Perform self-diagnostic test forever
rem ================================================================


:LOOP
nmake -f VCmakefile check
if errorlevel 1 goto :ERROR
echo OK
goto :LOOP


:ERROR
echo Error
exit /b 1


rem END OF FILE
