package cn.edu.ruc.iir.rainbow.seek.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.seek.receiver.ReceiverSeekEva;
import cn.edu.ruc.iir.rainbow.seek.cmd.CmdSeekEva;

public class InvokerSeekEva extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdSeekEva();
        command.setReceiver(new ReceiverSeekEva());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating GENERATE_DDL command.", e);
        }
    }
}
