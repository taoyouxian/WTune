package cn.edu.ruc.iir.rainbow.eva.invoker;

import cn.edu.ruc.iir.rainbow.common.cmd.Command;
import cn.edu.ruc.iir.rainbow.common.cmd.Invoker;
import cn.edu.ruc.iir.rainbow.common.exception.CommandException;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionHandler;
import cn.edu.ruc.iir.rainbow.common.exception.ExceptionType;
import cn.edu.ruc.iir.rainbow.eva.receiver.ReceiverWorkloadEva;
import cn.edu.ruc.iir.rainbow.eva.cmd.CmdWorkloadEva;

public class InvokerWorkloadEva extends Invoker
{
    /**
     * create this.command and set receiver for it
     */
    @Override
    protected void createCommands()
    {
        // combine command to proper receiver
        Command command = new CmdWorkloadEva();
        command.setReceiver(new ReceiverWorkloadEva());
        try
        {
            this.addCommand(command);
        } catch (CommandException e)
        {
            ExceptionHandler.Instance().log(ExceptionType.ERROR,
                    "error when creating WORKLOAD_EVALUATION command.", e);
        }
    }
}
