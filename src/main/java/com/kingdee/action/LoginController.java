package com.kingdee.action;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.ModelAndView;

/**
 * Created by zyq on 16/11/6.
 */
@Controller
@RequestMapping("/login")
public class LoginController {

    @RequestMapping("/main.do")
    public ModelAndView login() {
        ModelAndView mv = new ModelAndView("main");
        return mv;
    }
}
