package cn.edu.ruc.iir.rainbow.layout;

import org.junit.Test;

public class TestExtend
{
    public class A
    {
        private int id = 0;

        public int getId()
        {
            return id;
        }

        public void setId(int id)
        {
            this.id = id;
        }
    }

    public class B extends A
    {
        private int id;

        public int getId()
        {
            return id;
        }

        public void setId(int id)
        {
            this.id = id;
        }

        public void func ()
        {
            super.setId(1);
            System.out.println(this.getId());
        }
    }

    @Test
    public void test ()
    {
        B b = new B();
        b.func();
    }
}
