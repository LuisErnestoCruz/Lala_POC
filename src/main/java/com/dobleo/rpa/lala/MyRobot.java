package com.dobleo.rpa.lala;

import com.novayre.jidoka.client.api.IJidokaRobot;
import com.novayre.jidoka.client.api.IJidokaServer;
import com.novayre.jidoka.client.api.IRobot;
import com.novayre.jidoka.client.api.JidokaFactory;
import com.novayre.jidoka.client.api.annotations.Robot;
import com.novayre.jidoka.windows.api.EShowWindowState;
import com.novayre.jidoka.windows.api.IWindows;

/**
 * My robot
 * @author jidoka
 *
 */
@Robot
public class MyRobot implements IRobot {

	/**
	 * Pause between actions like persons do
	 */
	private static final int PAUSE = 500;
	
	/**
	 * Server
	 */
	private IJidokaServer<?> server;
	
	/**
	 * Windows module
	 */
	private IWindows windows;
	
	/**
	 * Current item index, base-1
	 */
	private int currentItem = 1;
	
	/**
	 * Action "start"
	 * @return
	 * @throws Exception
	 */
	public void start() throws Exception {
		
		server = (IJidokaServer<?>) JidokaFactory.getServer();
		
		windows = IJidokaRobot.getInstance(this);
		
		// we set standard pause after writing or managing mouse
		windows.typingPause(PAUSE);
		windows.mousePause(PAUSE);
		
		// log parameters example
		server.getParameters().entrySet().forEach((e) -> {
			server.debug(String.format("Parámetro [%s] = [%s]", e.getKey(), e.getValue()));
		});

		// other log types availables
		server.warn("Warn example");
		server.error("And error example");

		// we set number of items
		// on an actual robot, this number can be got from an application,
		// an Excel datasheet, etc.
		server.setNumberOfItems(1);
	}

	/**
	 * Action "openNotepad"
	 * @return
	 * @throws Exception
	 */
	public void openNotepad() throws Exception {
		
		windows.pause(PAUSE);

		// Win+R
		windows.typeText(windows.getKeyboardSequence()
				.pressWindows().type("r").releaseWindows());
		
		// we write "notepad"
		windows.typeText("notepad");
		
		// press return key using IKeyboardSequence
		windows.typeText(windows.getKeyboardSequence().typeReturn());
		
		windows.showWindow(windows.getWindow(".*(notepad|bloc).*").gethWnd(), EShowWindowState.SW_MAXIMIZE);

		// we do an explicit pause
		windows.pause(PAUSE);
	}
	
	/**
	 * Action "processItem"
	 * @return
	 * @throws Exception
	 */
	public void processItem() throws Exception {

		String item = "this is a test";

		// we inform that we are beginning the processing of an item
		server.setCurrentItem(currentItem, item);
		
		// we write the text into notepad
		windows.typeText(item);
		
		// press return key, we use IKeyboard instead of IKeyboardSequence
		windows.keyboard().enter();

		// we inform the item processing result
		server.setCurrentItemResultToOK();
		
		// we save the screenshot, it can be viewed in robot execution trace page on the console
		server.sendScreen(String.format("Snapshot over %s", item));
	}
	
	/**
	 * Action "moreData"
	 * @return
	 * @throws Exception
	 */
	public String moreData() throws Exception {
		return "no";
	}
	
	/**
	 * Action "closeNotepad"
	 * @return
	 * @throws Exception
	 */
	public void closeNotepad() throws Exception {
		
		// press alt+F4 + pause + "n"
		windows.typeText(windows.getKeyboardSequence().typeAltF(4).pause().type("n"));
		
		// another form
		//windows.keyboard().altF(4).pause().type("n");
	}
	
	/**
	 * Action "end"
	 * @return
	 * @throws Exception
	 */
	public void end() throws Exception {
	}
	
	/**
	 * @see com.novayre.jidoka.client.api.IRobot#cleanUp()
	 */
	/*
	@Override
	public String[] cleanUp() throws Exception {
		return new String[0];
	}
	*/
}
