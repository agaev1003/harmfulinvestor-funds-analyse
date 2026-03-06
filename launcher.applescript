on run
  try
    set appPath to POSIX path of (path to me)
    set projectDir to do shell script "/usr/bin/dirname " & quoted form of appPath
    set launcherPath to projectDir & "/launch-app.sh"

    set resultText to do shell script "/bin/zsh " & quoted form of launcherPath

    if resultText contains "__NODE_MISSING__" then
      display dialog "Node.js не найден.\n\nУстановите Node.js LTS: https://nodejs.org/" buttons {"OK"} default button "OK" with icon stop
      return
    end if

    if resultText contains "__NPM_INSTALL_FAILED__" then
      display dialog "Не удалось установить зависимости.\n\nСмотрите лог: /tmp/fund-analytics-install.log" buttons {"OK"} default button "OK" with icon stop
      return
    end if

  on error errMsg number errNum
    display dialog "Ошибка запуска (" & errNum & "): " & errMsg buttons {"OK"} default button "OK" with icon stop
  end try
end run
