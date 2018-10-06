package cn.edu.ruc.iir.rainbow.seek.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.seek.receiver.ReceiverGenFile;
import cn.edu.ruc.iir.rainbow.seek.cmd.CmdGenFile;

public class InvokerGenFile extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdGenFile();
        command.setReceiver(new ReceiverGenFile());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating GENERATE_File command.", e);
        }
    }
}
