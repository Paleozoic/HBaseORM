package com.maxplus1.test.base;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath:config/open/beans.xml")
public class BaseTest {

    @Before
    public void _before(){}

    @After
    public void _after(){}

    @Test
    public void test(){

    }

}
